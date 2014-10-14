package mongoimport

import (
	"errors"
	"fmt"
	"github.com/mongodb/mongo-tools/common/bsonutil"
	"github.com/mongodb/mongo-tools/common/json"
	"github.com/mongodb/mongo-tools/common/log"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strings"
	"sync"
)

// JSONInputReader is an implementation of InputReader that reads documents
// in JSON format.
type JSONInputReader struct {
	// IsArray indicates if the JSON import is an array of JSON documents
	// or not
	IsArray bool
	// Decoder is used to read the next valid JSON documents from the input source
	Decoder *json.Decoder
	// numProcessed indicates the number of JSON documents processed
	numProcessed int64
	// readOpeningBracket indicates if the underlying io.Reader has consumed
	// an opening bracket from the input source. Used to prevent errors when
	// a JSON input source contains just '[]'
	readOpeningBracket bool
	// expectedByte is used to store the next expected valid character for JSON
	// array imports
	expectedByte byte
	// bytesFromReader is used to store the next byte read from the Reader for
	// JSON array imports
	bytesFromReader []byte
	// separatorReader is used for JSON arrays to look for a valid array
	// separator. It is a reader consisting of the decoder's buffer and the
	// underlying reader
	separatorReader io.Reader
	// document is used to hold the decoded JSON document as a bson.M
	document bson.M
}

const (
	JSON_ARRAY_START = '['
	JSON_ARRAY_SEP   = ','
	JSON_ARRAY_END   = ']'
)

var (
	// ErrNoOpeningBracket means that the input source did not contain any
	// opening brace - returned only if --jsonArray is passed in.
	ErrNoOpeningBracket = errors.New("bad JSON array format - found no " +
		"opening bracket '[' in input source")

	// ErrNoClosingBracket means that the input source did not contain any
	// closing brace - returned only if --jsonArray is passed in.
	ErrNoClosingBracket = errors.New("bad JSON array format - found no " +
		"closing bracket ']' in input source")
)

// NewJSONInputReader creates a new JSONInputReader in array mode if specified,
// configured to read data to the given io.Reader
func NewJSONInputReader(isArray bool, in io.Reader) *JSONInputReader {
	return &JSONInputReader{
		IsArray:            isArray,
		Decoder:            json.NewDecoder(in),
		readOpeningBracket: false,
		bytesFromReader:    make([]byte, 1),
	}
}

// SetHeader is a no-op for JSON imports
func (jsonImporter *JSONInputReader) SetHeader(hasHeaderLine bool) error {
	return nil
}

// GetHeaders is a no-op for JSON imports
func (jsonImporter *JSONInputReader) GetHeaders() []string {
	return nil
}

// ReadHeadersFromSource is a no-op for JSON imports
func (jsonImporter *JSONInputReader) ReadHeadersFromSource() ([]string, error) {
	return nil, nil
}

// ReadDocument reads a line of input with the JSON representation of a document
// and writes the BSON equivalent to the provided channel
func (jsonImporter *JSONInputReader) ReadDocument(readChan chan bson.D, errChan chan error) {
	rawChan := make(chan []byte, numWorkers)
	var err error
	go func() {
		for {
			if jsonImporter.IsArray {
				if err = jsonImporter.readJSONArraySeparator(); err != nil {
					close(rawChan)
					if err == io.EOF {
						errChan <- err
					}
					jsonImporter.numProcessed++
					errChan <- fmt.Errorf("error reading separator after document #%v: %v", jsonImporter.numProcessed, err)
				}
			}
			rawBytes, err := jsonImporter.Decoder.ScanObject()
			if err != nil {
				close(rawChan)
				errChan <- err
				return
			}
			rawChan <- rawBytes
			jsonImporter.numProcessed++
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer func() {
				wg.Done()
				if r := recover(); r != nil {
					log.Logf(0, "error decoding JSON: %v", r)
				}
			}()
			jsonImporter.decodeJSON(rawChan, readChan)
		}()
	}
	wg.Wait()
	close(readChan)
}

// readJSONArraySeparator is a helper method used to process JSON arrays. It is
// used to read any of the valid separators for a JSON array and flag invalid
// characters.
//
// It will read a byte at a time until it finds an expected character after
// which it returns control to the caller.
//
// TODO: single byte sized scans are inefficient!
//
// It will also return immediately if it finds any error (including EOF). If it
// reads a JSON_ARRAY_END byte, as a validity check it will continue to scan the
// input source until it hits an error (including EOF) to ensure the entire
// input source content is a valid JSON array
func (jsonImporter *JSONInputReader) readJSONArraySeparator() error {
	jsonImporter.expectedByte = JSON_ARRAY_SEP
	if jsonImporter.numProcessed == 0 {
		jsonImporter.expectedByte = JSON_ARRAY_START
	}

	var readByte byte
	scanp := 0
	jsonImporter.separatorReader = io.MultiReader(jsonImporter.Decoder.Buffered(), jsonImporter.Decoder.R)
	for readByte != jsonImporter.expectedByte {
		n, err := jsonImporter.separatorReader.Read(jsonImporter.bytesFromReader)
		scanp += n
		if n == 0 || err != nil {
			if err == io.EOF {
				return ErrNoClosingBracket
			}
			return err
		}
		readByte = jsonImporter.bytesFromReader[0]

		if readByte == JSON_ARRAY_END {
			// if we read the end of the JSON array, ensure we have no other
			// non-whitespace characters at the end of the array
			for {
				_, err = jsonImporter.separatorReader.Read(jsonImporter.bytesFromReader)
				if err != nil {
					// takes care of the '[]' case
					if !jsonImporter.readOpeningBracket {
						return ErrNoOpeningBracket
					}
					return err
				}
				readString := string(jsonImporter.bytesFromReader[0])
				if strings.TrimSpace(readString) != "" {
					return fmt.Errorf("bad JSON array format - found '%v' "+
						"after '%v' in input source", readString,
						string(JSON_ARRAY_END))
				}
			}
		}

		// this will catch any invalid inter JSON object byte that occurs in the
		// input source
		if !(readByte == JSON_ARRAY_SEP ||
			strings.TrimSpace(string(readByte)) == "" ||
			readByte == JSON_ARRAY_START ||
			readByte == JSON_ARRAY_END) {
			if jsonImporter.expectedByte == JSON_ARRAY_START {
				return ErrNoOpeningBracket
			}
			return fmt.Errorf("bad JSON array format - found '%v' outside "+
				"JSON object/array in input source", string(readByte))
		}
	}
	// adjust the buffer to account for read bytes
	if scanp < len(jsonImporter.Decoder.Buf) {
		jsonImporter.Decoder.Buf = jsonImporter.Decoder.Buf[scanp:]
	} else {
		jsonImporter.Decoder.Buf = []byte{}
	}
	jsonImporter.readOpeningBracket = true
	return nil
}

// decodeJSON reads in data from the rawChan channel and creates a BSON document
// based on the record. It sends this document on the readChan channel if there
// are no errors. If any error is encountered, it sends this on the errChan
// channel and returns immediately
func (jsonImporter *JSONInputReader) decodeJSON(rawChan chan []byte, readChan chan bson.D) {
	var bsonD bson.D
	for rawBytes := range rawChan {
		document, err := json.UnmarshalBsonD(rawBytes)
		if err != nil {
			panic(fmt.Sprintf("error unmarshalling bytes on document #%v: %v", jsonImporter.numProcessed, err))
		}
		log.Logf(2, "got line: %v", document)
		// TODO: could move this to decode.go
		if bsonD, err = bsonutil.GetExtendedBsonD(document); err != nil {
			panic(fmt.Sprintf("error getting extended BSON for document #%v: %v", jsonImporter.numProcessed, err))
		}
		log.Logf(3, "got extended line: %#v", bsonD)
		readChan <- bsonD
	}
}
