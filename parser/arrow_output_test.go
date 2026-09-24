package main

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func sampleArrowEvent(sequence int) outputEvent {
	return outputEvent{
		EventID: "event-" + string(rune('0'+sequence)), EventEpochUS: 1789906364540116,
		RawEventType: "WriteRowsEventV2", Operation: "INSERT", DatabaseName: "fixture",
		TableName: "rows_abi", TableMapID: 87, SchemaVersionID: strings.Repeat("a", 64),
		ServerID: 44, ThreadID: 55, TransactionID: "transaction", GTID: "uuid:1",
		XID: "99", StartPosition: uint32(100 + sequence), EndPosition: uint32(200 + sequence),
		RowIndex: sequence, ExecutionTimeMS: 12, ErrorCode: 3, SQLKind: "INSERT",
		SQLText:    "INSERT INTO rows_abi VALUES (FROM_BASE64('AP/+'))",
		BeforeJSON: "", AfterJSON: `{"payload":{"$binary_base64":"AP/+","$length":3}}`,
		ColumnsJSON: `[{"index":0}]`, RowQuery: "INSERT", HeaderEpochUS: 1789906364000000,
		CommitEpochUS: 1789906365000000, TxnLastCommitted: 7, TxnSequenceNumber: 8,
		TxnLengthBytes: 900,
	}
}

func TestAtomicArrowOutputWritesCanonicalBatches(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parser.arrow")
	output, err := newAtomicArrowOutput(path, 1, 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 2; index++ {
		event := sampleArrowEvent(index)
		if index == 1 {
			event.SQLText = string([]byte{0xff})
		}
		if err := output.Encode(event); err != nil {
			output.Abort()
			t.Fatal(err)
		}
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".part"); !os.IsNotExist(err) {
		t.Fatalf("staging link remains after publication: %v", err)
	}

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
	if reader.NumRecords() != 2 {
		t.Fatalf("batch row bound was not enforced: got %d batches", reader.NumRecords())
	}
	if !reader.Schema().Equal(parserArrowSchema) {
		t.Fatalf("unexpected Arrow schema: %s", reader.Schema())
	}
	first, err := reader.RecordBatchAt(0)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	if first.NumRows() != 1 || first.NumCols() != parserArrowTransportFields {
		t.Fatalf("unexpected record shape: %d x %d", first.NumRows(), first.NumCols())
	}
	if got := first.Column(0).(*array.String).Value(0); got != "event-0" {
		t.Fatalf("event identity changed: %q", got)
	}
	if got := first.Column(6).(*array.Uint64).Value(0); got != 87 {
		t.Fatalf("unsigned table_map_id changed: %d", got)
	}
	if got := first.Column(22).(*array.String).Value(0); !strings.Contains(got, "$binary_base64") {
		t.Fatalf("binary body changed: %q", got)
	}
	for index := 30; index < parserArrowTransportFields; index++ {
		if !first.Column(index).IsNull(0) {
			t.Fatalf("audit-only parser field %d is not null", index)
		}
	}
	second, err := reader.RecordBatchAt(1)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	if got := second.Column(19).(*array.String).Value(0); got != "\ufffd" {
		t.Fatalf("invalid UTF-8 did not match JSON replacement semantics: %q", got)
	}
}

func TestAtomicArrowOutputNeverOverwritesOrLeavesFailedPublication(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "parser.arrow")
	if err := os.WriteFile(path, []byte("owned"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newAtomicArrowOutput(path, 1, 1<<20, 1<<20); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing final output was not rejected: %v", err)
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != "owned" {
		t.Fatalf("existing final output changed: %q %v", body, err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".part", []byte("owned-part"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newAtomicArrowOutput(path, 1, 1<<20, 1<<20); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("existing partial output was not rejected: %v", err)
	}
}

func TestAtomicArrowOutputRejectsOversizedRecordAndUnsignedOverflow(t *testing.T) {
	tests := map[string]struct {
		mutate func(*outputEvent)
		marker string
	}{
		"record":  {func(event *outputEvent) { event.SQLText = strings.Repeat("x", 2000) }, "batch limit"},
		"integer": {func(event *outputEvent) { event.TxnLengthBytes = math.MaxInt64 + 1 }, "signed 64-bit"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "parser.arrow")
			output, err := newAtomicArrowOutput(path, 10, 1024, 1<<20)
			if err != nil {
				t.Fatal(err)
			}
			event := sampleArrowEvent(0)
			test.mutate(&event)
			if err := output.Encode(event); err == nil || !strings.Contains(err.Error(), test.marker) {
				t.Fatalf("invalid event was not rejected: %v", err)
			}
			output.Abort()
			if _, err := os.Stat(path); !os.IsNotExist(err) {
				t.Fatalf("failed output was published: %v", err)
			}
			if _, err := os.Stat(path + ".part"); !os.IsNotExist(err) {
				t.Fatalf("failed staging output remains: %v", err)
			}
		})
	}
}

func TestAtomicArrowOutputEnforcesReaderResourceContracts(t *testing.T) {
	t.Run("decoded bytes", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "parser.arrow")
		output, err := newAtomicArrowOutput(path, 10, 1024, 1024)
		if err != nil {
			t.Fatal(err)
		}
		defer output.Abort()
		var limitErr error
		for index := 0; index < 10 && limitErr == nil; index++ {
			limitErr = output.Encode(sampleArrowEvent(index))
		}
		if limitErr == nil || !strings.Contains(limitErr.Error(), "decoded estimate") {
			t.Fatalf("decoded byte limit was not enforced: %v", limitErr)
		}
	})

	t.Run("record batches", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "parser.arrow")
		output, err := newAtomicArrowOutput(path, 1, 1<<20, maximumArrowOutputBytes)
		if err != nil {
			t.Fatal(err)
		}
		for index := 0; index <= maximumArrowRecordBatches; index++ {
			if err := output.Encode(sampleArrowEvent(index)); err != nil {
				output.Abort()
				t.Fatal(err)
			}
		}
		if err := output.Close(); err == nil || !strings.Contains(err.Error(), "record-batch limit") {
			t.Fatalf("record-batch limit was not enforced: %v", err)
		}
		for _, candidate := range []string{path, path + ".part"} {
			if _, err := os.Stat(candidate); !os.IsNotExist(err) {
				t.Fatalf("bounded output failure retained %s: %v", candidate, err)
			}
		}
	})
}

func TestBoundedArrowFileRejectsWriteBeforeCrossingLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bounded.arrow")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	bounded := &boundedArrowFile{file: file, limit: 3}
	if written, err := bounded.Write([]byte("four")); written != 0 || err == nil {
		t.Fatalf("oversized write crossed limit: bytes=%d err=%v", written, err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if stat, err := os.Stat(path); err != nil || stat.Size() != 0 {
		t.Fatalf("rejected write changed file: stat=%v err=%v", stat, err)
	}
}

func TestRunArrowFailurePublishesNothing(t *testing.T) {
	directory := t.TempDir()
	input := filepath.Join(directory, "bad.binlog")
	output := filepath.Join(directory, "parser.arrow")
	if err := os.WriteFile(input, []byte("not-a-binlog"), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"--input", input, "--source-file-id", "fixture", "--arrow-output", output}, bytes.NewReader(nil), &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "not a MySQL binlog") {
		t.Fatalf("bad input was not rejected: code=%d stderr=%q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("Arrow mode leaked NDJSON to stdout: %q", stdout.String())
	}
	for _, path := range []string{output, output + ".part"} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed parse published %s: %v", path, err)
		}
	}

	stdout.Reset()
	stderr.Reset()
	code = run([]string{"--input", input, "--source-file-id", "fixture", "--arrow-output", output,
		"--output-dir", filepath.Join(directory, "chunks")}, bytes.NewReader(nil), &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "mutually exclusive") {
		t.Fatalf("mixed output modes were not rejected: code=%d stderr=%q", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("rejected Arrow mode leaked stdout: %q", stdout.String())
	}
}
