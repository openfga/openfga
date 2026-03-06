package commands

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/config"
)

// TupleBatch is a batch of tuples read from a file.
type TupleBatch struct {
	Tuples []*openfgav1.TupleKey
}

// FileReader reads tuples from a source file and yields them in batches.
type FileReader interface {
	// ReadBatches streams tuple batches from the source. The channel is closed when reading is complete.
	// Errors are sent on the error channel. At most one error is sent.
	ReadBatches(ctx context.Context, batchSize int) (<-chan TupleBatch, <-chan error)
}

// NewFileReader creates a FileReader based on the source URI and format.
func NewFileReader(source, format string) (FileReader, error) {
	u, err := url.Parse(source)
	if err != nil {
		return nil, fmt.Errorf("invalid source URI: %w", err)
	}

	var opener fileOpener
	switch u.Scheme {
	case "file", "":
		opener = localFileOpener{}
	default:
		return nil, fmt.Errorf("unsupported URI scheme: %s", u.Scheme)
	}

	switch format {
	case "jsonl":
		return &jsonlReader{source: source, opener: opener, uri: u}, nil
	case "json":
		return &jsonReader{source: source, opener: opener, uri: u}, nil
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

// fileOpener abstracts file opening for testability.
type fileOpener interface {
	Open(u *url.URL) (*os.File, error)
}

type localFileOpener struct{}

func (localFileOpener) Open(u *url.URL) (*os.File, error) {
	path := u.Path
	if u.Scheme == "" {
		path = u.String()
	}
	return os.Open(path)
}

// tupleJSON represents a tuple as written in import files.
type tupleJSON struct {
	Object    string         `json:"object"`
	Relation  string         `json:"relation"`
	User      string         `json:"user"`
	Condition *conditionJSON `json:"condition,omitempty"`
}

type conditionJSON struct {
	Name    string                 `json:"name"`
	Context map[string]interface{} `json:"context,omitempty"`
}

func (t *tupleJSON) toTupleKey() (*openfgav1.TupleKey, error) {
	tk := &openfgav1.TupleKey{
		Object:   t.Object,
		Relation: t.Relation,
		User:     t.User,
	}
	if t.Condition != nil {
		cond := &openfgav1.RelationshipCondition{
			Name: t.Condition.Name,
		}
		if len(t.Condition.Context) > 0 {
			contextStruct, err := structpb.NewStruct(t.Condition.Context)
			if err != nil {
				return nil, fmt.Errorf("invalid condition context: %w", err)
			}
			cond.Context = contextStruct
		}
		tk.Condition = cond
	}
	return tk, nil
}

// jsonlReader reads tuples line-by-line from a JSONL file.
type jsonlReader struct {
	source string
	opener fileOpener
	uri    *url.URL
}

func (r *jsonlReader) ReadBatches(ctx context.Context, batchSize int) (<-chan TupleBatch, <-chan error) {
	if batchSize <= 0 {
		batchSize = config.DefaultImportDataBatchSize
	}
	batchCh := make(chan TupleBatch, 2)
	errCh := make(chan error, 1)

	go func() {
		defer close(batchCh)
		defer close(errCh)

		f, err := r.opener.Open(r.uri)
		if err != nil {
			errCh <- fmt.Errorf("failed to open file %s: %w", r.source, err)
			return
		}
		defer f.Close()

		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024) // 1MB max line size

		batch := make([]*openfgav1.TupleKey, 0, batchSize)
		for scanner.Scan() {
			if ctx.Err() != nil {
				return
			}

			line := scanner.Bytes()
			if len(line) == 0 {
				continue // skip empty lines
			}

			var t tupleJSON
			if err := json.Unmarshal(line, &t); err != nil {
				errCh <- fmt.Errorf("failed to parse JSONL line: %w", err)
				return
			}

			tk, err := t.toTupleKey()
			if err != nil {
				errCh <- err
				return
			}

			batch = append(batch, tk)
			if len(batch) >= batchSize {
				select {
				case batchCh <- TupleBatch{Tuples: batch}:
				case <-ctx.Done():
					return
				}
				batch = make([]*openfgav1.TupleKey, 0, batchSize)
			}
		}

		if err := scanner.Err(); err != nil {
			errCh <- fmt.Errorf("error reading file: %w", err)
			return
		}

		// Send remaining tuples
		if len(batch) > 0 {
			select {
			case batchCh <- TupleBatch{Tuples: batch}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return batchCh, errCh
}

// jsonReader reads tuples from a JSON array file.
type jsonReader struct {
	source string
	opener fileOpener
	uri    *url.URL
}

func (r *jsonReader) ReadBatches(ctx context.Context, batchSize int) (<-chan TupleBatch, <-chan error) {
	if batchSize <= 0 {
		batchSize = config.DefaultImportDataBatchSize
	}
	batchCh := make(chan TupleBatch, 2)
	errCh := make(chan error, 1)

	go func() {
		defer close(batchCh)
		defer close(errCh)

		f, err := r.opener.Open(r.uri)
		if err != nil {
			errCh <- fmt.Errorf("failed to open file %s: %w", r.source, err)
			return
		}
		defer f.Close()

		decoder := json.NewDecoder(f)
		// Read opening bracket
		token, err := decoder.Token()
		if err != nil {
			errCh <- fmt.Errorf("failed to read JSON array start: %w", err)
			return
		}
		if delim, ok := token.(json.Delim); !ok || delim != '[' {
			errCh <- fmt.Errorf("expected JSON array, got %v", token)
			return
		}

		batch := make([]*openfgav1.TupleKey, 0, batchSize)
		for decoder.More() {
			if ctx.Err() != nil {
				return
			}

			var t tupleJSON
			if err := decoder.Decode(&t); err != nil {
				errCh <- fmt.Errorf("failed to decode JSON tuple: %w", err)
				return
			}

			tk, err := t.toTupleKey()
			if err != nil {
				errCh <- err
				return
			}

			batch = append(batch, tk)
			if len(batch) >= batchSize {
				select {
				case batchCh <- TupleBatch{Tuples: batch}:
				case <-ctx.Done():
					return
				}
				batch = make([]*openfgav1.TupleKey, 0, batchSize)
			}
		}

		// Send remaining tuples
		if len(batch) > 0 {
			select {
			case batchCh <- TupleBatch{Tuples: batch}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return batchCh, errCh
}
