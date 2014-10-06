package mongoimport

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongoimport/csv"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
	"sync"
)

// CSVInputReader is a struct that implements the InputReader interface for a
// CSV input source
type CSVInputReader struct {
	// Fields is a list of field names in the BSON documents to be imported
	Fields []string
	// csvReader is the underlying reader used to read data in from the CSV
	// or TSV file
	csvReader *csv.Reader
	// numProcessed indicates the number of CSV documents processed
	numProcessed int64
	// csvRecord stores each line of input we read from the underlying reader
	csvRecord []string
	// document is used to hold the decoded JSON document as a bson.M
	document bson.M
}

// NewCSVInputReader returns a CSVInputReader configured to read input from the
// given io.Reader, extracting the specified fields only.
func NewCSVInputReader(fields []string, in io.Reader) *CSVInputReader {
	csvReader := csv.NewReader(in)
	// allow variable number of fields in document
	csvReader.FieldsPerRecord = -1
	csvReader.TrimLeadingSpace = true
	return &CSVInputReader{
		Fields:    fields,
		csvReader: csvReader,
	}
}

// SetHeader sets the header field for a CSV
func (csvImporter *CSVInputReader) SetHeader(hasHeaderLine bool) (err error) {
	fields, err := validateHeaders(csvImporter, hasHeaderLine)
	if err != nil {
		return err
	}
	csvImporter.Fields = fields
	return nil
}

// GetHeaders returns the current header fields for a CSV importer
func (csvImporter *CSVInputReader) GetHeaders() []string {
	return csvImporter.Fields
}

// ReadHeadersFromSource reads the header field from the CSV importer's reader
func (csvImporter *CSVInputReader) ReadHeadersFromSource() ([]string, error) {
	return csvImporter.csvReader.Read()
}

// ReadDocument reads a line of input with the CSV representation of a document
// and writes the BSON equivalent to the provided read channel; if it encounters
// an error in reading, it sends that error on the error channel.
func (csvImporter *CSVInputReader) ReadDocument(readChan chan bson.M, errChan chan error) {
	csvRecordChan := make(chan []string, numWorkers)
	var err error

	go func() {
		for {
			csvImporter.csvRecord, err = csvImporter.csvReader.Read()
			if err != nil {
				close(csvRecordChan)
				csvImporter.numProcessed++
				if err == io.EOF {
					errChan <- err
				}
				errChan <- fmt.Errorf("read error on entry #%v: %v", csvImporter.numProcessed, err)
			}
			csvRecordChan <- csvImporter.csvRecord
			csvImporter.numProcessed++
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					log.Logf(0, "error decoding CSV: %v", r)
				}
			}()
			csvImporter.sendCSV(csvRecordChan, readChan)
		}()
	}
	wg.Wait()
	close(readChan)
}

// sendCSV reads in data from the csvRecordChan channel and creates a BSON document
// based on the record. It sends this document on the readChan channel if there
// are no errors. If any error is encountered, it sends this on the errChan
// channel and returns immediately
func (csvImporter *CSVInputReader) sendCSV(csvRecordChan chan []string, readChan chan bson.M) {
	var key string
	var parsedValue interface{}
	var document bson.D

	for csvRecord := range csvRecordChan {
		log.Logf(2, "got line: %v", csvRecord)
		document = bson.D{}
		for index, token := range csvRecord {
			parsedValue = getParsedValue(token)
			if index < len(csvImporter.Fields) {
				// for nested fields - in the form "a.b.c", ensure
				// that the value is set accordingly
				if strings.Index(csvImporter.Fields[index], ".") != -1 {
					// setNestedValue(csvImporter.Fields[index], parsedValue, document)
				} else {
					document = append(document, bson.DocElem{csvImporter.Fields[index], parsedValue})
				}
			} else {
				key = "field" + string(index)
				if util.StringSliceContains(csvImporter.Fields, key) {
					panic(fmt.Sprintf("Duplicate header name - on %v - for token #%v ('%v') in document #%v",
						key, index+1, parsedValue, csvImporter.numProcessed))
				}
				document = append(document, bson.DocElem{csvImporter.Fields[index], parsedValue})
			}
		}
		readChan <- bson.M{"body": "'''Wei-chi''' may refer to:\n*The [[game of go]]\n*The [[Chinese w", "page_id": 747205, "user": "TheQ Editor", "title": "Wei-chi"}
	}
}
