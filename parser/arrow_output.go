package main

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"unicode/utf8"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

const (
	defaultArrowBatchRows      = 4096
	defaultArrowBatchBytes     = int64(32 << 20)
	defaultArrowMaxFileBytes   = int64(128 << 20)
	maximumArrowBatchRows      = 1_000_000
	maximumArrowBatchBytes     = int64(128 << 20)
	maximumArrowOutputBytes    = int64(128 << 20)
	minimumArrowByteLimit      = int64(1024)
	maximumArrowRecordBatches  = 4096
	parserArrowTransportFields = 41
)

var parserArrowSchema = arrow.NewSchema([]arrow.Field{
	{Name: "event_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "event_epoch_us", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "raw_event_type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "operation", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "database_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "table_map_id", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
	{Name: "schema_version_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "server_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "thread_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "transaction_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "gtid", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "xid", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "start_position", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "end_position", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "row_index", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "execution_time_ms", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "error_code", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "sql_kind", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "sql_text", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "sql_bytes_base64", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "before_json", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "after_json", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "columns_json", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "row_query", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "header_epoch_us", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "commit_epoch_us", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "txn_last_committed", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "txn_sequence_number", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "txn_length_bytes", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "connection_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "connection_name", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "database_account", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "execution_status", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "error_message", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "affected_rows", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "started_epoch_us", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "finished_epoch_us", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "batch_id", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "statement_index", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "transaction_context_id", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

type boundedArrowFile struct {
	file  *os.File
	limit int64
	bytes int64
}

func (w *boundedArrowFile) Write(data []byte) (int, error) {
	if int64(len(data)) > w.limit-w.bytes {
		return 0, fmt.Errorf("Arrow IPC file exceeds %d-byte limit", w.limit)
	}
	n, err := w.file.Write(data)
	w.bytes += int64(n)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return n, err
}

type atomicArrowOutput struct {
	finalPath    string
	partialPath  string
	file         *os.File
	bounded      *boundedArrowFile
	writer       *ipc.FileWriter
	builder      *array.RecordBuilder
	batchRows    int
	batchBytes   int64
	rows         int
	bytes        int64
	batches      int
	decodedBytes int64
	closed       bool
}

func validateArrowLimits(batchRows int, batchBytes, maxFileBytes int64) error {
	if batchRows < 1 || batchRows > maximumArrowBatchRows {
		return fmt.Errorf("arrow batch rows must be between 1 and %d", maximumArrowBatchRows)
	}
	if batchBytes < minimumArrowByteLimit || batchBytes > maximumArrowBatchBytes {
		return fmt.Errorf("arrow batch bytes must be between %d and %d", minimumArrowByteLimit, maximumArrowBatchBytes)
	}
	if maxFileBytes < minimumArrowByteLimit || maxFileBytes > maximumArrowOutputBytes {
		return fmt.Errorf("arrow file bytes must be between %d and %d", minimumArrowByteLimit, maximumArrowOutputBytes)
	}
	return nil
}

func pathMustNotExist(path string) error {
	_, err := os.Lstat(path)
	if err == nil {
		return fmt.Errorf("output path already exists: %s", path)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect output path %q: %w", path, err)
	}
	return nil
}

func newAtomicArrowOutput(path string, batchRows int, batchBytes, maxFileBytes int64) (*atomicArrowOutput, error) {
	if err := validateArrowLimits(batchRows, batchBytes, maxFileBytes); err != nil {
		return nil, err
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve arrow output path: %w", err)
	}
	parent := filepath.Dir(absolutePath)
	parentStat, err := os.Stat(parent)
	if err != nil {
		return nil, fmt.Errorf("inspect arrow output directory: %w", err)
	}
	if !parentStat.IsDir() {
		return nil, fmt.Errorf("arrow output parent is not a directory: %s", parent)
	}
	partialPath := absolutePath + ".part"
	if err := pathMustNotExist(absolutePath); err != nil {
		return nil, err
	}
	if err := pathMustNotExist(partialPath); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(partialPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create arrow output: %w", err)
	}
	bounded := &boundedArrowFile{file: file, limit: maxFileBytes}
	writer, err := ipc.NewFileWriter(bounded, ipc.WithSchema(parserArrowSchema), ipc.WithAllocator(memory.NewGoAllocator()))
	if err != nil {
		_ = file.Close()
		_ = os.Remove(partialPath)
		return nil, fmt.Errorf("initialize Arrow IPC writer: %w", err)
	}
	return &atomicArrowOutput{
		finalPath: absolutePath, partialPath: partialPath, file: file, bounded: bounded,
		writer: writer, builder: array.NewRecordBuilder(memory.NewGoAllocator(), parserArrowSchema),
		batchRows: batchRows, batchBytes: batchBytes,
	}, nil
}

func arrowString(value string) string {
	if utf8.ValidString(value) {
		return value
	}
	return string([]rune(value))
}

func arrowEventBytes(event outputEvent) int64 {
	total := int64(256)
	for _, value := range []string{
		event.EventID, event.RawEventType, event.Operation, event.DatabaseName, event.TableName,
		event.SchemaVersionID, event.TransactionID, event.GTID, event.XID, event.SQLKind,
		event.SQLText, event.SQLBytesBase64, event.BeforeJSON, event.AfterJSON,
		event.ColumnsJSON, event.RowQuery,
	} {
		total += int64(len(arrowString(value)))
	}
	return total
}

func checkedInt64(name string, value uint64) (int64, error) {
	if value > math.MaxInt64 {
		return 0, fmt.Errorf("%s exceeds signed 64-bit transport range: %d", name, value)
	}
	return int64(value), nil
}

func checkedInt32(name string, value int) (int32, error) {
	if value < math.MinInt32 || value > math.MaxInt32 {
		return 0, fmt.Errorf("%s exceeds signed 32-bit transport range: %d", name, value)
	}
	return int32(value), nil
}

func (a *atomicArrowOutput) Encode(event outputEvent) error {
	if a.closed {
		return errors.New("Arrow output is closed")
	}
	lastCommitted := event.TxnLastCommitted
	sequenceNumber := event.TxnSequenceNumber
	txnLength, err := checkedInt64("txn_length_bytes", event.TxnLengthBytes)
	if err != nil {
		return err
	}
	rowIndex, err := checkedInt32("row_index", event.RowIndex)
	if err != nil {
		return err
	}
	errorCode, err := checkedInt32("error_code", int(event.ErrorCode))
	if err != nil {
		return err
	}
	recordBytes := arrowEventBytes(event)
	if recordBytes > a.batchBytes {
		return fmt.Errorf("Arrow record estimate %d exceeds %d-byte batch limit", recordBytes, a.batchBytes)
	}
	if recordBytes > a.bounded.limit-a.decodedBytes {
		return fmt.Errorf("Arrow decoded estimate exceeds %d-byte file contract", a.bounded.limit)
	}
	if a.rows > 0 && (a.rows >= a.batchRows || recordBytes > a.batchBytes-a.bytes) {
		if err := a.flush(); err != nil {
			return err
		}
	}

	fields := a.builder.Fields()
	appendString := func(index int, value string) {
		fields[index].(*array.StringBuilder).Append(arrowString(value))
	}
	appendInt64 := func(index int, value int64) {
		fields[index].(*array.Int64Builder).Append(value)
	}
	appendString(0, event.EventID)
	appendInt64(1, event.EventEpochUS)
	appendString(2, event.RawEventType)
	appendString(3, event.Operation)
	appendString(4, event.DatabaseName)
	appendString(5, event.TableName)
	fields[6].(*array.Uint64Builder).Append(event.TableMapID)
	appendString(7, event.SchemaVersionID)
	appendInt64(8, int64(event.ServerID))
	appendInt64(9, int64(event.ThreadID))
	appendString(10, event.TransactionID)
	appendString(11, event.GTID)
	appendString(12, event.XID)
	appendInt64(13, int64(event.StartPosition))
	appendInt64(14, int64(event.EndPosition))
	fields[15].(*array.Int32Builder).Append(rowIndex)
	appendInt64(16, event.ExecutionTimeMS)
	fields[17].(*array.Int32Builder).Append(errorCode)
	appendString(18, event.SQLKind)
	appendString(19, event.SQLText)
	appendString(20, event.SQLBytesBase64)
	appendString(21, event.BeforeJSON)
	appendString(22, event.AfterJSON)
	appendString(23, event.ColumnsJSON)
	appendString(24, event.RowQuery)
	appendInt64(25, event.HeaderEpochUS)
	appendInt64(26, event.CommitEpochUS)
	appendInt64(27, lastCommitted)
	appendInt64(28, sequenceNumber)
	appendInt64(29, txnLength)
	for index := 30; index < parserArrowTransportFields; index++ {
		fields[index].AppendNull()
	}
	a.rows++
	a.bytes += recordBytes
	a.decodedBytes += recordBytes
	return nil
}

func (a *atomicArrowOutput) flush() error {
	if a.rows == 0 {
		return nil
	}
	if a.batches >= maximumArrowRecordBatches {
		return fmt.Errorf("Arrow output exceeds %d record-batch limit", maximumArrowRecordBatches)
	}
	record := a.builder.NewRecordBatch()
	defer record.Release()
	if record.NumRows() != int64(a.rows) {
		return fmt.Errorf("Arrow batch row mismatch: record=%d buffered=%d", record.NumRows(), a.rows)
	}
	if err := a.writer.Write(record); err != nil {
		return fmt.Errorf("write Arrow IPC batch: %w", err)
	}
	a.rows = 0
	a.bytes = 0
	a.batches++
	return nil
}

func syncArrowDirectory(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (a *atomicArrowOutput) releaseBuilder() {
	if a.builder != nil {
		a.builder.Release()
		a.builder = nil
	}
}

func (a *atomicArrowOutput) Abort() {
	if a.closed {
		return
	}
	a.closed = true
	a.releaseBuilder()
	if a.file != nil {
		_ = a.file.Close()
		a.file = nil
	}
	_ = os.Remove(a.partialPath)
}

func (a *atomicArrowOutput) closeFailure(err error) error {
	a.Abort()
	return err
}

func (a *atomicArrowOutput) Close() error {
	if a.closed {
		return nil
	}
	if err := a.flush(); err != nil {
		return a.closeFailure(err)
	}
	if err := a.writer.Close(); err != nil {
		return a.closeFailure(fmt.Errorf("close Arrow IPC writer: %w", err))
	}
	a.writer = nil
	a.releaseBuilder()
	if err := a.file.Sync(); err != nil {
		return a.closeFailure(fmt.Errorf("sync Arrow IPC file: %w", err))
	}
	if err := a.file.Close(); err != nil {
		a.file = nil
		return a.closeFailure(fmt.Errorf("close Arrow IPC file: %w", err))
	}
	a.file = nil
	if err := os.Link(a.partialPath, a.finalPath); err != nil {
		return a.closeFailure(fmt.Errorf("publish Arrow IPC file without overwrite: %w", err))
	}
	parent := filepath.Dir(a.finalPath)
	if err := syncArrowDirectory(parent); err != nil {
		_ = os.Remove(a.finalPath)
		return a.closeFailure(fmt.Errorf("sync published Arrow IPC directory: %w", err))
	}
	if err := os.Remove(a.partialPath); err != nil {
		_ = os.Remove(a.finalPath)
		return a.closeFailure(fmt.Errorf("remove Arrow IPC staging link: %w", err))
	}
	if err := syncArrowDirectory(parent); err != nil {
		_ = os.Remove(a.finalPath)
		return a.closeFailure(fmt.Errorf("sync Arrow IPC staging cleanup: %w", err))
	}
	a.closed = true
	return nil
}
