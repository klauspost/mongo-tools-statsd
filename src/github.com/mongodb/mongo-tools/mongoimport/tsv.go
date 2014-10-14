package mongoimport

import (
	"bufio"
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
	"sync"
)

const (
	entryDelimiter = '\n'
	tokenSeparator = "\t"
)

// TSVInputReader is a struct that implements the InputReader interface for a
// TSV input source
type TSVInputReader struct {
	// Fields is a list of field names in the BSON documents to be imported
	Fields []string
	// tsvReader is the underlying reader used to read data in from the TSV
	// or TSV file
	tsvReader *bufio.Reader
	// numProcessed indicates the number of TSV documents processed
	numProcessed int64
	// tsvRecord stores each line of input we read from the underlying reader
	tsvRecord string
	// document is used to hold the decoded JSON document as a bson.M
	document bson.M
}

// NewTSVInputReader returns a TSVInputReader configured to read input from the
// given io.Reader, extracting the specified fields only.
func NewTSVInputReader(fields []string, in io.Reader) *TSVInputReader {
	return &TSVInputReader{
		Fields:    fields,
		tsvReader: bufio.NewReader(in),
	}
}

// SetHeader sets the header field for a TSV
func (tsvImporter *TSVInputReader) SetHeader(hasHeaderLine bool) (err error) {
	fields, err := validateHeaders(tsvImporter, hasHeaderLine)
	if err != nil {
		return err
	}
	tsvImporter.Fields = fields
	return nil
}

// GetHeaders returns the current header fields for a TSV importer
func (tsvImporter *TSVInputReader) GetHeaders() []string {
	return tsvImporter.Fields
}

// ReadHeadersFromSource reads the header field from the TSV importer's reader
func (tsvImporter *TSVInputReader) ReadHeadersFromSource() ([]string, error) {
	unsortedHeaders := []string{}
	stringHeaders, err := tsvImporter.tsvReader.ReadString(entryDelimiter)
	if err != nil {
		return nil, err
	}
	tokenizedHeaders := strings.Split(stringHeaders, tokenSeparator)
	for _, header := range tokenizedHeaders {
		unsortedHeaders = append(unsortedHeaders, strings.TrimSpace(header))
	}
	return unsortedHeaders, nil
}

// ReadDocument reads a line of input with the TSV representation of a document
// and writes the BSON equivalent to the provided channel
func (tsvImporter *TSVInputReader) ReadDocument(readChan chan bson.D, errChan chan error) {
	tsvRecordChan := make(chan string, numWorkers)
	var err error

	go func() {
		for {
			tsvImporter.tsvRecord, err = tsvImporter.tsvReader.ReadString(entryDelimiter)
			if err != nil {
				close(tsvRecordChan)
				if err == io.EOF {
					errChan <- err
				}
				tsvImporter.numProcessed++
				errChan <- fmt.Errorf("read error on entry #%v: %v", tsvImporter.numProcessed, err)
			}
			tsvRecordChan <- tsvImporter.tsvRecord
			tsvImporter.numProcessed++
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					log.Logf(0, "error decoding TSV: %v", r)
				}
			}()
			tsvImporter.sendTSV(tsvRecordChan, readChan)
		}()
	}
	wg.Wait()
	close(readChan)
}

// sendTSV reads in data from the tsvRecordChan channel and creates a BSON document
// based on the record. It sends this document on the readChan channel if there
// are no errors. If any error is encountered, it sends this on the errChan
// channel and returns immediately
func (tsvImporter *TSVInputReader) sendTSV(tsvRecordChan chan string, readChan chan bson.D) {
	var key string
	var document bson.D
	for tsvRecord := range tsvRecordChan {
		log.Logf(2, "got line: %v", tsvRecord)

		// strip the trailing '\r\n' from ReadString
		if len(tsvRecord) != 0 {
			tsvRecord = strings.TrimRight(tsvRecord, "\r\n")
		}
		document = bson.D{}
		for index, token := range strings.Split(tsvRecord, tokenSeparator) {
			parsedValue := getParsedValue(token)
			if index < len(tsvImporter.Fields) {
				if strings.Index(tsvImporter.Fields[index], ".") != -1 {
					setNestedValue(tsvImporter.Fields[index], parsedValue, &document)
				} else {
					document = append(document, bson.DocElem{tsvImporter.Fields[index], parsedValue})
				}
			} else {
				key = "field" + string(index)
				if util.StringSliceContains(tsvImporter.Fields, key) {
					panic(fmt.Sprintf("Duplicate header name - on %v - for token #%v ('%v') in document #%v",
						key, index+1, parsedValue, tsvImporter.numProcessed))
				}
				document = append(document, bson.DocElem{tsvImporter.Fields[index], parsedValue})
			}
		}
		readChan <- document
	}
}
