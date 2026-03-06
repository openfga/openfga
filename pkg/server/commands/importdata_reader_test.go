package commands

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSONLReaderReadBatches(t *testing.T) {
	t.Run("reads_tuples_in_batches", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "tuples.jsonl")
		content := `{"object":"document:1","relation":"viewer","user":"user:alice"}
{"object":"document:2","relation":"viewer","user":"user:bob"}
{"object":"document:3","relation":"editor","user":"user:charlie"}`
		require.NoError(t, os.WriteFile(path, []byte(content), 0644))

		reader, err := NewFileReader("file://"+path, "jsonl")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 2)

		var allTuples int
		var batches int
		for batch := range batchCh {
			allTuples += len(batch.Tuples)
			batches++
		}
		require.Empty(t, drainErrors(errCh))
		require.Equal(t, 3, allTuples)
		require.Equal(t, 2, batches) // 2 in first batch, 1 in second
	})

	t.Run("handles_empty_file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "empty.jsonl")
		require.NoError(t, os.WriteFile(path, []byte(""), 0644))

		reader, err := NewFileReader("file://"+path, "jsonl")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 100)

		var count int
		for range batchCh {
			count++
		}
		require.Empty(t, drainErrors(errCh))
		require.Equal(t, 0, count)
	})

	t.Run("handles_malformed_json", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bad.jsonl")
		content := `{"object":"document:1","relation":"viewer","user":"user:alice"}
not valid json`
		require.NoError(t, os.WriteFile(path, []byte(content), 0644))

		reader, err := NewFileReader("file://"+path, "jsonl")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 100)

		// Drain batch channel
		for range batchCh {
		}
		errs := drainErrors(errCh)
		require.Len(t, errs, 1)
		require.Contains(t, errs[0].Error(), "failed to parse JSONL line")
	})

	t.Run("reads_tuples_with_conditions", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "cond.jsonl")
		content := `{"object":"document:1","relation":"viewer","user":"user:alice","condition":{"name":"ip_check","context":{"ip":"192.168.1.1"}}}`
		require.NoError(t, os.WriteFile(path, []byte(content), 0644))

		reader, err := NewFileReader("file://"+path, "jsonl")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 100)

		var tuples int
		for batch := range batchCh {
			for _, tk := range batch.Tuples {
				require.Equal(t, "ip_check", tk.GetCondition().GetName())
				tuples++
			}
		}
		require.Empty(t, drainErrors(errCh))
		require.Equal(t, 1, tuples)
	})

	t.Run("skips_empty_lines", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "gaps.jsonl")
		content := `{"object":"document:1","relation":"viewer","user":"user:alice"}

{"object":"document:2","relation":"viewer","user":"user:bob"}
`
		require.NoError(t, os.WriteFile(path, []byte(content), 0644))

		reader, err := NewFileReader("file://"+path, "jsonl")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 100)

		var count int
		for batch := range batchCh {
			count += len(batch.Tuples)
		}
		require.Empty(t, drainErrors(errCh))
		require.Equal(t, 2, count)
	})
}

func TestJSONReaderReadBatches(t *testing.T) {
	t.Run("reads_json_array", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "tuples.json")
		content := `[
			{"object":"document:1","relation":"viewer","user":"user:alice"},
			{"object":"document:2","relation":"editor","user":"user:bob"}
		]`
		require.NoError(t, os.WriteFile(path, []byte(content), 0644))

		reader, err := NewFileReader("file://"+path, "json")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 100)

		var count int
		for batch := range batchCh {
			count += len(batch.Tuples)
		}
		require.Empty(t, drainErrors(errCh))
		require.Equal(t, 2, count)
	})

	t.Run("handles_empty_array", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "empty.json")
		require.NoError(t, os.WriteFile(path, []byte("[]"), 0644))

		reader, err := NewFileReader("file://"+path, "json")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 100)

		var count int
		for range batchCh {
			count++
		}
		require.Empty(t, drainErrors(errCh))
		require.Equal(t, 0, count)
	})
}

func TestNewFileReader(t *testing.T) {
	t.Run("unsupported_scheme", func(t *testing.T) {
		_, err := NewFileReader("s3://bucket/file.jsonl", "jsonl")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported URI scheme")
	})

	t.Run("unsupported_format", func(t *testing.T) {
		_, err := NewFileReader("file:///tmp/file.csv", "csv")
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported format")
	})

	t.Run("file_not_found", func(t *testing.T) {
		reader, err := NewFileReader("file:///nonexistent/path/tuples.jsonl", "jsonl")
		require.NoError(t, err)

		ctx := context.Background()
		batchCh, errCh := reader.ReadBatches(ctx, 100)

		for range batchCh {
		}
		errs := drainErrors(errCh)
		require.Len(t, errs, 1)
		require.Contains(t, errs[0].Error(), "failed to open file")
	})
}

func TestContextCancellation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tuples.jsonl")
	// Write many tuples
	var content string
	for i := 0; i < 100; i++ {
		content += `{"object":"document:1","relation":"viewer","user":"user:alice"}` + "\n"
	}
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))

	reader, err := NewFileReader("file://"+path, "jsonl")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	batchCh, _ := reader.ReadBatches(ctx, 2)

	// Read one batch then cancel
	<-batchCh
	cancel()

	// Drain - should stop quickly
	for range batchCh {
	}
}

func drainErrors(errCh <-chan error) []error {
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}
	return errs
}
