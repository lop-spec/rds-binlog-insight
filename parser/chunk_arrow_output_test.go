package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func arrowChunkACKs(sequences ...int) *strings.Reader {
	var lines strings.Builder
	for _, sequence := range sequences {
		payload, _ := json.Marshal(arrowChunkACK{
			Protocol: arrowChunkACKProtocol,
			Sequence: sequence,
			Status:   "ok",
		})
		lines.Write(payload)
		lines.WriteByte('\n')
	}
	return strings.NewReader(lines.String())
}

func readArrowChunkRows(t *testing.T, path string) int64 {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, err := ipc.NewFileReader(file)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if !reader.Schema().Equal(parserArrowSchema) {
		t.Fatalf("Arrow chunk schema changed: %s", reader.Schema())
	}
	var rows int64
	for index := 0; index < reader.NumRecords(); index++ {
		record, err := reader.RecordBatchAt(index)
		if err != nil {
			t.Fatal(err)
		}
		rows += record.NumRows()
		record.Release()
	}
	return rows
}

func TestChunkDestinationRejectsUnsafeSourceIdentifiers(t *testing.T) {
	directory := t.TempDir()
	for _, value := range []string{
		"", ".", "..", "../escape", `C:escape`, "bad\nid", "设备", " trailing", strings.Repeat("x", 129),
	} {
		t.Run(fmt.Sprintf("%q", value), func(t *testing.T) {
			if _, _, err := validateChunkDestination(directory, value); err == nil {
				t.Fatalf("unsafe source identifier was accepted: %q", value)
			}
		})
	}
	if _, actual, err := validateChunkDestination(directory, "source-id_1.test"); err != nil || actual != "source-id_1.test" {
		t.Fatalf("safe source identifier was rejected: %q %v", actual, err)
	}
}

func TestChunkedNDJSONOutputUsesTheSameBoundedAtomicProtocol(t *testing.T) {
	directory := t.TempDir()
	var manifests bytes.Buffer
	output, err := newChunkedOutput(
		directory,
		"source-id",
		2,
		1<<20,
		&manifests,
		strings.NewReader("ok\nok\n"),
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := output.Encode(sampleArrowEvent(index)); err != nil {
			output.Abort()
			t.Fatal(err)
		}
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(manifests.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two NDJSON manifests, got %d", len(lines))
	}
	for index, line := range lines {
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatal(err)
		}
		if len(raw) != 6 {
			t.Fatalf("NDJSON manifest fields changed: %#v", raw)
		}
		var manifest chunkManifest
		if err := json.Unmarshal([]byte(line), &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Protocol != chunkManifestProtocol || manifest.Format != ndjsonChunkFormat ||
			manifest.Sequence != index || manifest.Bytes <= 0 || manifest.Bytes > 1<<20 {
			t.Fatalf("unexpected NDJSON manifest: %#v", manifest)
		}
		expectedRows := 2
		if index == 1 {
			expectedRows = 1
		}
		if manifest.Rows != expectedRows {
			t.Fatalf("NDJSON chunk %d rows changed: %d", index, manifest.Rows)
		}
		body, err := os.ReadFile(manifest.Path)
		if err != nil {
			t.Fatal(err)
		}
		if int64(len(body)) != manifest.Bytes ||
			len(strings.Split(strings.TrimSpace(string(body)), "\n")) != expectedRows {
			t.Fatalf("NDJSON chunk %d content does not match its manifest", index)
		}
		if _, err := os.Stat(manifest.Path + ".part"); !os.IsNotExist(err) {
			t.Fatalf("NDJSON chunk %d staging path remains: %v", index, err)
		}
	}
	if _, err := newChunkedOutput(
		directory, "other-source", 1, maximumArrowOutputBytes+1,
		&bytes.Buffer{}, strings.NewReader("ok\n"),
	); err == nil {
		t.Fatal("NDJSON chunk accepted a byte bound above the collector hard limit")
	}
}

func TestChunkedNDJSONOutputFailsClosedWithoutACKOrOverwrite(t *testing.T) {
	t.Run("bad ACK removes unowned publication", func(t *testing.T) {
		directory := t.TempDir()
		output, err := newChunkedOutput(
			directory, "source-id", 1, 1<<20, &bytes.Buffer{}, strings.NewReader("wrong\n"),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := output.Encode(sampleArrowEvent(0)); err == nil || !strings.Contains(err.Error(), "unexpected chunk ACK") {
			t.Fatalf("bad ACK was accepted: %v", err)
		}
		output.Abort()
		for _, suffix := range []string{".ndjson", ".ndjson.part"} {
			path := filepath.Join(directory, "source-id-000000"+suffix)
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("failed ACK retained %s: %v", path, err)
			}
		}
		if output.publishedPath != "" {
			t.Fatalf("Abort retained unacknowledged ownership: %q", output.publishedPath)
		}
	})

	t.Run("existing final is never replaced or removed", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "source-id-000000.ndjson")
		if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
			t.Fatal(err)
		}
		output, err := newChunkedOutput(
			directory, "source-id", 1, 1<<20, &bytes.Buffer{}, strings.NewReader("ok\n"),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := output.Encode(sampleArrowEvent(0)); err == nil || !strings.Contains(err.Error(), "without overwrite") {
			t.Fatalf("existing final was accepted: %v", err)
		}
		output.Abort()
		body, err := os.ReadFile(path)
		if err != nil || string(body) != "owned" {
			t.Fatalf("existing final changed: %q %v", body, err)
		}
	})
}

func TestChunkedArrowOutputPublishesStrictOrderedManifests(t *testing.T) {
	directory := t.TempDir()
	var manifests bytes.Buffer
	output, err := newChunkedArrowOutput(
		directory,
		"source-id",
		2,
		1<<20,
		&manifests,
		arrowChunkACKs(0, 1),
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 3; index++ {
		if err := output.Encode(sampleArrowEvent(index)); err != nil {
			output.Abort()
			t.Fatal(err)
		}
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimSpace(manifests.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two manifests, got %d: %q", len(lines), manifests.String())
	}
	for index, line := range lines {
		var raw map[string]any
		if err := json.Unmarshal([]byte(line), &raw); err != nil {
			t.Fatal(err)
		}
		expectedKeys := []string{
			"protocol", "format", "sequence", "path", "rows", "bytes", "decoded_bytes",
		}
		if len(raw) != len(expectedKeys) {
			t.Fatalf("manifest fields changed: %#v", raw)
		}
		for _, key := range expectedKeys {
			if _, ok := raw[key]; !ok {
				t.Fatalf("manifest is missing %q: %#v", key, raw)
			}
		}
		var manifest chunkManifest
		if err := json.Unmarshal([]byte(line), &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Protocol != chunkManifestProtocol || manifest.Format != arrowChunkFormat || manifest.Sequence != index {
			t.Fatalf("unexpected manifest identity: %#v", manifest)
		}
		expectedRows := 2
		if index == 1 {
			expectedRows = 1
		}
		if manifest.Rows != expectedRows || manifest.DecodedBytes <= 0 || manifest.DecodedBytes > 1<<20 {
			t.Fatalf("unexpected manifest bounds: %#v", manifest)
		}
		stat, err := os.Stat(manifest.Path)
		if err != nil {
			t.Fatal(err)
		}
		if stat.Size() != manifest.Bytes || stat.Size() > 1<<20 {
			t.Fatalf("physical chunk bound changed: manifest=%d actual=%d", manifest.Bytes, stat.Size())
		}
		if rows := readArrowChunkRows(t, manifest.Path); rows != int64(expectedRows) {
			t.Fatalf("chunk %d rows changed: %d", index, rows)
		}
		if _, err := os.Stat(manifest.Path + ".part"); !os.IsNotExist(err) {
			t.Fatalf("chunk %d staging path remains: %v", index, err)
		}
	}
}

func TestChunkedArrowOutputFailsClosedWithoutACKOrOverwrite(t *testing.T) {
	t.Run("bad ACK removes unowned publication", func(t *testing.T) {
		directory := t.TempDir()
		var manifests bytes.Buffer
		output, err := newChunkedArrowOutput(
			directory, "source-id", 1, 1<<20, &manifests,
			strings.NewReader(`{"protocol":"parser-chunk-ack-v1","sequence":9,"status":"ok"}`+"\n"),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := output.Encode(sampleArrowEvent(0)); err == nil || !strings.Contains(err.Error(), "unexpected Arrow chunk ACK") {
			t.Fatalf("bad ACK was accepted: %v", err)
		}
		output.Abort()
		if output.publishedPath != "" {
			t.Fatalf("Abort retained unacknowledged ownership: %q", output.publishedPath)
		}
		for _, suffix := range []string{".arrow", ".arrow.part"} {
			path := filepath.Join(directory, "source-id-000000"+suffix)
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("failed ACK retained %s: %v", path, err)
			}
		}
	})

	t.Run("close failure remains abortable", func(t *testing.T) {
		directory := t.TempDir()
		output, err := newChunkedArrowOutput(
			directory, "source-id", 2, 1<<20, &bytes.Buffer{},
			strings.NewReader(`{"protocol":"parser-chunk-ack-v1","sequence":7,"status":"ok"}`+"\n"),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := output.Encode(sampleArrowEvent(0)); err != nil {
			t.Fatal(err)
		}
		if err := output.Close(); err == nil {
			t.Fatal("invalid close ACK was accepted")
		}
		if output.closed {
			t.Fatal("failed Close marked the output closed before Abort")
		}
		output.Abort()
		if !output.closed || output.current != nil {
			t.Fatal("Abort did not clear the failed Close state")
		}
	})

	t.Run("existing final is never replaced", func(t *testing.T) {
		directory := t.TempDir()
		path := filepath.Join(directory, "source-id-000000.arrow")
		if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
			t.Fatal(err)
		}
		output, err := newChunkedArrowOutput(
			directory, "source-id", 2, 1<<20, &bytes.Buffer{}, arrowChunkACKs(0),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := output.Encode(sampleArrowEvent(0)); err == nil || !strings.Contains(err.Error(), "already exists") {
			t.Fatalf("existing final was not rejected: %v", err)
		}
		output.Abort()
		body, err := os.ReadFile(path)
		if err != nil || string(body) != "owned" {
			t.Fatalf("existing final changed: %q %v", body, err)
		}
	})

	t.Run("oversized record creates no artifact", func(t *testing.T) {
		directory := t.TempDir()
		output, err := newChunkedArrowOutput(
			directory, "source-id", 2, minimumArrowByteLimit, &bytes.Buffer{}, arrowChunkACKs(0),
		)
		if err != nil {
			t.Fatal(err)
		}
		event := sampleArrowEvent(0)
		event.SQLText = strings.Repeat("x", 2000)
		if err := output.Encode(event); err == nil || !strings.Contains(err.Error(), "chunk limit") {
			t.Fatalf("oversized record was accepted: %v", err)
		}
		output.Abort()
		entries, err := os.ReadDir(directory)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 0 {
			t.Fatalf("oversized record left artifacts: %#v", entries)
		}
	})
}

func TestDecodeArrowChunkACKRejectsUnknownAndTrailingData(t *testing.T) {
	for _, raw := range []string{
		`{"protocol":"parser-chunk-ack-v1","sequence":0,"status":"ok","extra":1}` + "\n",
		`{"protocol":"parser-chunk-ack-v1","sequence":0,"status":"ok"} {}` + "\n",
	} {
		if err := decodeArrowChunkACK(raw, 0); err == nil {
			t.Fatalf("invalid ACK was accepted: %q", raw)
		}
	}
}
