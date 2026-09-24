package main

import (
	"archive/tar"
	"archive/zip"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/klauspost/compress/zstd"
	"github.com/shopspring/decimal"
)

var errUnsupportedInput = errors.New("not a MySQL binlog or supported archive")

const (
	formatDescriptionEventType       = byte(15)
	binlogEventHeaderSize            = 19
	binlogEventSizeOffset            = 9
	binlogEventFlagsOffset           = 17
	binlogChecksumLength             = 4
	binlogChecksumAlgorithmLength    = 1
	binlogChecksumAlgorithmCRC32     = byte(1)
	logEventBinlogInUseFlag          = uint16(0x0001)
	maxFormatDescriptionEventSize    = 1 << 20
	minFormatDescriptionEventSize    = binlogEventHeaderSize + 2 + 50 + 4 + 1
	formatDescriptionChecksumTailLen = binlogChecksumAlgorithmLength + binlogChecksumLength
)

type checksumResult struct {
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
	CRC64     string `json:"crc64"`
}

func checksumReader(reader io.Reader) (checksumResult, error) {
	sha := sha256.New()
	crc := crc64.New(crc64.MakeTable(crc64.ECMA))
	size, err := io.Copy(io.MultiWriter(sha, crc), reader)
	if err != nil {
		return checksumResult{}, fmt.Errorf("checksum input: %w", err)
	}
	return checksumResult{
		SizeBytes: size,
		SHA256:    hex.EncodeToString(sha.Sum(nil)),
		CRC64:     strconv.FormatUint(crc.Sum64(), 10),
	}, nil
}

type outputEvent struct {
	EventID         string `json:"event_id"`
	EventEpochUS    int64  `json:"event_epoch_us"`
	RawEventType    string `json:"raw_event_type"`
	Operation       string `json:"operation"`
	DatabaseName    string `json:"database_name"`
	TableName       string `json:"table_name"`
	ServerID        uint32 `json:"server_id"`
	ThreadID        uint32 `json:"thread_id"`
	TransactionID   string `json:"transaction_id"`
	GTID            string `json:"gtid"`
	XID             string `json:"xid"`
	StartPosition   uint32 `json:"start_position"`
	EndPosition     uint32 `json:"end_position"`
	RowIndex        int    `json:"row_index"`
	ExecutionTimeMS int64  `json:"execution_time_ms"`
	ErrorCode       uint16 `json:"error_code"`
	SQLKind         string `json:"sql_kind"`
	SQLText         string `json:"sql_text"`
	SQLBytesBase64  string `json:"sql_bytes_base64"`
	BeforeJSON      string `json:"before_json"`
	AfterJSON       string `json:"after_json"`
	ColumnsJSON     string `json:"columns_json"`
	RowQuery        string `json:"row_query"`
}

type columnDescription struct {
	Index         int    `json:"index"`
	Name          string `json:"name"`
	TypeID        byte   `json:"type_id"`
	Metadata      uint16 `json:"metadata"`
	NullableKnown bool   `json:"nullable_known"`
	Nullable      bool   `json:"nullable"`
	PrimaryKey    bool   `json:"primary_key"`
}

type extractor struct {
	encoder            *json.Encoder
	afterEncode        func() error
	sourceFileID       string
	flavor             string
	outputSequence     uint64
	emitted            uint64
	currentGTID        string
	currentTransaction string
	currentThreadID    uint32
	currentRowQuery    string
	transactionEpochUS int64
	lastEpochUS        int64
	// slim keeps only what the ClickHouse row store needs: no pseudo SQL,
	// column metadata or base64 SQL, row_query cut to slimRowQueryRunes code
	// points (the store keeps leftUTF8(row_query, 65536) anyway) and no
	// transaction boundary records. Skipped records still advance
	// outputSequence, so every emitted event_id equals the full output.
	slim bool
}

const slimRowQueryRunes = 65536

func truncateRunes(value string, limit int) string {
	count := 0
	for index := range value {
		if count == limit {
			return value[:index]
		}
		count++
	}
	return value
}

func newExtractor(writer io.Writer, sourceFileID, flavor string) *extractor {
	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)
	return &extractor{encoder: encoder, sourceFileID: sourceFileID, flavor: flavor}
}

func (x *extractor) resetStreamState() {
	x.currentGTID = ""
	x.currentTransaction = ""
	x.currentThreadID = 0
	x.currentRowQuery = ""
	x.transactionEpochUS = 0
}

func eventPositions(header *replication.EventHeader) (uint32, uint32) {
	end := header.LogPos
	if end >= header.EventSize {
		return end - header.EventSize, end
	}
	return 0, end
}

func (x *extractor) epochUS(header *replication.EventHeader) int64 {
	if x.transactionEpochUS > 0 {
		return x.transactionEpochUS
	}
	if header.Timestamp > 0 {
		value := int64(header.Timestamp) * 1_000_000
		x.lastEpochUS = value
		return value
	}
	return x.lastEpochUS
}

func (x *extractor) fallbackTransaction(start uint32) string {
	if x.currentTransaction != "" {
		return x.currentTransaction
	}
	x.currentTransaction = x.sourceFileID + ":" + strconv.FormatUint(uint64(start), 10)
	return x.currentTransaction
}

func (x *extractor) clearTransaction() {
	x.currentGTID = ""
	x.currentTransaction = ""
	x.currentThreadID = 0
	x.currentRowQuery = ""
	x.transactionEpochUS = 0
}

func (x *extractor) emit(record outputEvent) error {
	if record.EventEpochUS <= 0 {
		return fmt.Errorf("event %s at position %d has no valid timestamp", record.RawEventType, record.StartPosition)
	}
	x.outputSequence++
	if x.slim && record.Operation == "TRANSACTION" {
		return nil
	}
	stable := strings.Join([]string{
		x.sourceFileID,
		strconv.FormatUint(uint64(record.StartPosition), 10),
		strconv.FormatUint(uint64(record.EndPosition), 10),
		strconv.FormatUint(x.outputSequence, 10),
		strconv.Itoa(record.RowIndex),
		record.Operation,
	}, "\x1f")
	sum := sha256.Sum256([]byte(stable))
	record.EventID = hex.EncodeToString(sum[:])
	if err := x.encoder.Encode(record); err != nil {
		return fmt.Errorf("write JSONL event: %w", err)
	}
	if x.afterEncode != nil {
		if err := x.afterEncode(); err != nil {
			return fmt.Errorf("publish JSONL event: %w", err)
		}
	}
	x.emitted++
	return nil
}

type chunkManifest struct {
	Path  string `json:"path"`
	Rows  int    `json:"rows"`
	Bytes int64  `json:"bytes"`
}

type chunkedOutput struct {
	outputDir       string
	sourceFileID    string
	maxLines        int
	maxBytes        int64
	manifestEncoder *json.Encoder
	ackReader       *bufio.Reader
	index           int
	file            *os.File
	buffered        *bufio.Writer
	partialPath     string
	finalPath       string
	rows            int
	bytes           int64
	closed          bool
}

func newChunkedOutput(
	outputDir string,
	sourceFileID string,
	maxLines int,
	maxBytes int64,
	manifestWriter io.Writer,
	ackReader io.Reader,
) (*chunkedOutput, error) {
	absoluteDir, err := filepath.Abs(strings.TrimSpace(outputDir))
	if err != nil {
		return nil, fmt.Errorf("resolve chunk output directory: %w", err)
	}
	stat, err := os.Stat(absoluteDir)
	if err != nil {
		return nil, fmt.Errorf("stat chunk output directory: %w", err)
	}
	if !stat.IsDir() {
		return nil, errors.New("chunk output path must be a directory")
	}
	sourceFileID = strings.TrimSpace(sourceFileID)
	if sourceFileID == "" ||
		sourceFileID == "." ||
		sourceFileID == ".." ||
		filepath.Base(sourceFileID) != sourceFileID ||
		strings.ContainsAny(sourceFileID, `/\`) {
		return nil, errors.New("source file identifier is not safe for chunk paths")
	}
	if maxLines <= 0 || maxBytes <= 0 {
		return nil, errors.New("chunk limits must be positive")
	}
	if manifestWriter == nil || ackReader == nil {
		return nil, errors.New("chunk manifest writer and ACK reader are required")
	}
	return &chunkedOutput{
		outputDir:       absoluteDir,
		sourceFileID:    sourceFileID,
		maxLines:        maxLines,
		maxBytes:        maxBytes,
		manifestEncoder: json.NewEncoder(manifestWriter),
		ackReader:       bufio.NewReader(ackReader),
	}, nil
}

func (c *chunkedOutput) openChunk() error {
	if c.closed {
		return errors.New("chunk output is closed")
	}
	if c.file != nil {
		return nil
	}
	filename := fmt.Sprintf("%s-%06d.ndjson", c.sourceFileID, c.index)
	c.finalPath = filepath.Join(c.outputDir, filename)
	c.partialPath = c.finalPath + ".part"
	for _, path := range []string{c.partialPath, c.finalPath} {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove stale chunk %q: %w", path, err)
		}
	}
	file, err := os.OpenFile(c.partialPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("create chunk %q: %w", c.partialPath, err)
	}
	c.file = file
	c.buffered = bufio.NewWriterSize(file, 1<<20)
	return nil
}

func (c *chunkedOutput) Write(value []byte) (int, error) {
	if err := c.openChunk(); err != nil {
		return 0, err
	}
	count, err := c.buffered.Write(value)
	c.bytes += int64(count)
	if err == nil && count != len(value) {
		err = io.ErrShortWrite
	}
	return count, err
}

func (c *chunkedOutput) recordComplete() error {
	if c.file == nil {
		return errors.New("record completed before chunk data was written")
	}
	c.rows++
	if c.rows >= c.maxLines || c.bytes >= c.maxBytes {
		return c.publish()
	}
	return nil
}

func (c *chunkedOutput) publish() error {
	if c.rows == 0 {
		return nil
	}
	if c.file == nil || c.buffered == nil {
		return errors.New("chunk state is incomplete")
	}
	if err := c.buffered.Flush(); err != nil {
		return fmt.Errorf("flush chunk: %w", err)
	}
	if err := c.file.Sync(); err != nil {
		return fmt.Errorf("sync chunk: %w", err)
	}
	if err := c.file.Close(); err != nil {
		return fmt.Errorf("close chunk: %w", err)
	}
	c.file = nil
	c.buffered = nil
	if err := os.Rename(c.partialPath, c.finalPath); err != nil {
		return fmt.Errorf("publish chunk: %w", err)
	}
	stat, err := os.Stat(c.finalPath)
	if err != nil {
		return fmt.Errorf("stat published chunk: %w", err)
	}
	manifest := chunkManifest{Path: c.finalPath, Rows: c.rows, Bytes: stat.Size()}
	c.index++
	c.partialPath = ""
	c.finalPath = ""
	c.rows = 0
	c.bytes = 0
	if err := c.manifestEncoder.Encode(manifest); err != nil {
		return fmt.Errorf("write chunk manifest: %w", err)
	}
	ack, err := c.ackReader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("wait for chunk ACK: %w", err)
	}
	if strings.TrimSpace(ack) != "ok" {
		return fmt.Errorf("unexpected chunk ACK %q", strings.TrimSpace(ack))
	}
	return nil
}

func (c *chunkedOutput) Close() error {
	if c.closed {
		return nil
	}
	defer func() { c.closed = true }()
	return c.publish()
}

func (c *chunkedOutput) Abort() {
	if c.closed {
		return
	}
	c.closed = true
	if c.file != nil {
		_ = c.file.Close()
	}
	if c.partialPath != "" {
		_ = os.Remove(c.partialPath)
	}
	c.file = nil
	c.buffered = nil
}

func byteText(value []byte) string {
	if utf8.Valid(value) {
		return string(value)
	}
	return "base64:" + base64.StdEncoding.EncodeToString(value)
}

func sqlText(value []byte) (string, string) {
	if utf8.Valid(value) {
		return string(value), ""
	}
	return "", base64.StdEncoding.EncodeToString(value)
}

func normalizeValue(value any) any {
	switch item := value.(type) {
	case nil:
		return nil
	case []byte:
		return map[string]any{
			"$binary_base64": base64.StdEncoding.EncodeToString(item),
			"$length":        len(item),
		}
	case decimal.Decimal:
		return map[string]any{"$decimal": item.String()}
	case *decimal.Decimal:
		if item == nil {
			return nil
		}
		return map[string]any{"$decimal": item.String()}
	case time.Time:
		return map[string]any{"$time_rfc3339_nano": item.UTC().Format(time.RFC3339Nano)}
	case *time.Time:
		if item == nil {
			return nil
		}
		return map[string]any{"$time_rfc3339_nano": item.UTC().Format(time.RFC3339Nano)}
	case float64:
		return map[string]any{
			"$float64": strconv.FormatFloat(item, 'g', -1, 64),
			"$ieee754": fmt.Sprintf("%016x", math.Float64bits(item)),
		}
	case float32:
		return map[string]any{
			"$float32": strconv.FormatFloat(float64(item), 'g', -1, 32),
			"$ieee754": fmt.Sprintf("%08x", math.Float32bits(item)),
		}
	case replication.FloatWithTrailingZero:
		floatValue := float64(item)
		return map[string]any{
			"$float64": strconv.FormatFloat(floatValue, 'g', -1, 64),
			"$ieee754": fmt.Sprintf("%016x", math.Float64bits(floatValue)),
		}
	case *replication.JsonDiff:
		if item == nil {
			return nil
		}
		return map[string]any{
			"$json_diff": map[string]any{
				"operation": item.Op.String(),
				"path":      item.Path,
				"value":     item.Value,
			},
		}
	case []any:
		result := make([]any, len(item))
		for index := range item {
			result[index] = normalizeValue(item[index])
		}
		return result
	case map[string]any:
		result := make(map[string]any, len(item))
		for key, child := range item {
			result[key] = normalizeValue(child)
		}
		return result
	default:
		return value
	}
}

func compactJSON(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func columnNames(table *replication.TableMapEvent) []string {
	count := int(table.ColumnCount)
	names := table.ColumnNameString()
	result := make([]string, count)
	for index := 0; index < count; index++ {
		if index < len(names) && names[index] != "" {
			result[index] = names[index]
		} else {
			result[index] = "@" + strconv.Itoa(index+1)
		}
	}
	return result
}

func skippedSet(skipped []int) map[int]struct{} {
	result := make(map[int]struct{}, len(skipped))
	for _, index := range skipped {
		result[index] = struct{}{}
	}
	return result
}

func rowObject(row []any, skipped []int, names []string) map[string]any {
	missing := skippedSet(skipped)
	result := make(map[string]any, len(row)-len(missing))
	for index := range row {
		if _, omitted := missing[index]; omitted {
			continue
		}
		name := "@" + strconv.Itoa(index+1)
		if index < len(names) {
			name = names[index]
		}
		result[name] = normalizeValue(row[index])
	}
	return result
}

func columnsDescription(table *replication.TableMapEvent, names []string) ([]columnDescription, error) {
	primary := make(map[uint64]struct{}, len(table.PrimaryKey))
	for _, index := range table.PrimaryKey {
		primary[index] = struct{}{}
	}
	result := make([]columnDescription, 0, table.ColumnCount)
	for index := 0; index < int(table.ColumnCount); index++ {
		typeID := byte(0)
		metadata := uint16(0)
		if index < len(table.ColumnType) {
			typeID = table.ColumnType[index]
		}
		if index < len(table.ColumnMeta) {
			metadata = table.ColumnMeta[index]
		}
		_, isPrimary := primary[uint64(index)]
		nullableKnown, nullable := table.Nullable(index)
		result = append(result, columnDescription{
			Index:         index,
			Name:          names[index],
			TypeID:        typeID,
			Metadata:      metadata,
			NullableKnown: nullableKnown,
			Nullable:      nullable,
			PrimaryKey:    isPrimary,
		})
	}
	return result, nil
}

func quoteIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func qualifiedTable(databaseName, tableName string) string {
	if databaseName == "" {
		return quoteIdentifier(tableName)
	}
	return quoteIdentifier(databaseName) + "." + quoteIdentifier(tableName)
}

func pseudoLiteral(value any) string {
	switch item := value.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(item, "\\", "\\\\"), "'", "''") + "'"
	case bool:
		if item {
			return "1"
		}
		return "0"
	case []byte:
		return "FROM_BASE64('" + base64.StdEncoding.EncodeToString(item) + "')"
	case decimal.Decimal:
		return item.String()
	case *decimal.Decimal:
		if item == nil {
			return "NULL"
		}
		return item.String()
	case time.Time:
		return "'" + item.UTC().Format(time.RFC3339Nano) + "'"
	case *replication.JsonDiff:
		if item == nil {
			return "NULL"
		}
		return "/* " + item.String() + " */ NULL"
	default:
		data, err := json.Marshal(item)
		if err == nil {
			return string(data)
		}
		return "/* unsupported value */ NULL"
	}
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func pseudoSQL(operation, databaseName, tableName string, before, after map[string]any) string {
	table := qualifiedTable(databaseName, tableName)
	switch operation {
	case "INSERT":
		keys := sortedKeys(after)
		identifiers := make([]string, len(keys))
		values := make([]string, len(keys))
		for index, key := range keys {
			identifiers[index] = quoteIdentifier(key)
			values[index] = pseudoLiteral(after[key])
		}
		return "INSERT INTO " + table + " (" + strings.Join(identifiers, ", ") + ") VALUES (" + strings.Join(values, ", ") + ");"
	case "UPDATE":
		setKeys := sortedKeys(after)
		whereKeys := sortedKeys(before)
		sets := make([]string, len(setKeys))
		wheres := make([]string, len(whereKeys))
		for index, key := range setKeys {
			sets[index] = quoteIdentifier(key) + " = " + pseudoLiteral(after[key])
		}
		for index, key := range whereKeys {
			wheres[index] = quoteIdentifier(key) + " <=> " + pseudoLiteral(before[key])
		}
		where := "/* before image unavailable */ 1 = 1"
		if len(wheres) > 0 {
			where = strings.Join(wheres, " AND ")
		}
		return "UPDATE " + table + " SET " + strings.Join(sets, ", ") + " WHERE " + where + " LIMIT 1;"
	case "DELETE":
		keys := sortedKeys(before)
		wheres := make([]string, len(keys))
		for index, key := range keys {
			wheres[index] = quoteIdentifier(key) + " <=> " + pseudoLiteral(before[key])
		}
		where := "/* before image unavailable */ 1 = 1"
		if len(wheres) > 0 {
			where = strings.Join(wheres, " AND ")
		}
		return "DELETE FROM " + table + " WHERE " + where + " LIMIT 1;"
	default:
		return ""
	}
}

func leadingKeyword(query string) string {
	remaining := strings.TrimSpace(query)
	for {
		switch {
		case strings.HasPrefix(remaining, "/*"):
			end := strings.Index(remaining[2:], "*/")
			if end < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[end+4:])
		case strings.HasPrefix(remaining, "--"):
			end := strings.IndexByte(remaining, '\n')
			if end < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[end+1:])
		case strings.HasPrefix(remaining, "#"):
			end := strings.IndexByte(remaining, '\n')
			if end < 0 {
				return ""
			}
			remaining = strings.TrimSpace(remaining[end+1:])
		default:
			fields := strings.Fields(remaining)
			if len(fields) == 0 {
				return ""
			}
			return strings.ToUpper(strings.Trim(fields[0], ";"))
		}
	}
}

func classifyQuery(query string) string {
	keyword := leadingKeyword(query)
	switch keyword {
	case "INSERT", "REPLACE":
		return "INSERT"
	case "UPDATE":
		return "UPDATE"
	case "DELETE":
		return "DELETE"
	case "CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME", "GRANT", "REVOKE":
		return "DDL"
	case "BEGIN", "COMMIT", "ROLLBACK", "XA", "SAVEPOINT":
		return "TRANSACTION"
	default:
		return "QUERY"
	}
}

func rowsOperation(eventType replication.EventType, rowType replication.EnumRowsEventType) (string, error) {
	if eventType == replication.PARTIAL_UPDATE_ROWS_EVENT {
		return "UPDATE", nil
	}
	switch rowType {
	case replication.EnumRowsEventTypeInsert:
		return "INSERT", nil
	case replication.EnumRowsEventTypeUpdate:
		return "UPDATE", nil
	case replication.EnumRowsEventTypeDelete:
		return "DELETE", nil
	default:
		return "", fmt.Errorf("unsupported rows event type %s", eventType.String())
	}
}

func (x *extractor) handleRows(
	binlogEvent *replication.BinlogEvent,
	event *replication.RowsEvent,
	overrideStart *uint32,
	overrideEnd *uint32,
) error {
	if event.Table == nil {
		return errors.New("rows event has no table map metadata")
	}
	start, end := eventPositions(binlogEvent.Header)
	if overrideStart != nil && overrideEnd != nil {
		start, end = *overrideStart, *overrideEnd
	}
	operation, err := rowsOperation(binlogEvent.Header.EventType, event.Type())
	if err != nil {
		return err
	}
	databaseName := byteText(event.Table.Schema)
	tableName := byteText(event.Table.Table)
	names := columnNames(event.Table)
	descriptions, err := columnsDescription(event.Table, names)
	if err != nil {
		return err
	}
	columnsJSON := ""
	if !x.slim {
		columnsJSON, err = compactJSON(descriptions)
		if err != nil {
			return fmt.Errorf("encode column metadata: %w", err)
		}
	}
	rowQuery := x.currentRowQuery
	if x.slim {
		rowQuery = truncateRunes(rowQuery, slimRowQueryRunes)
	}
	transactionID := x.fallbackTransaction(start)
	epoch := x.epochUS(binlogEvent.Header)
	if operation == "UPDATE" && len(event.Rows)%2 != 0 {
		return fmt.Errorf("update rows event has odd row-image count %d", len(event.Rows))
	}
	if len(event.SkippedColumns) != 0 && len(event.SkippedColumns) != len(event.Rows) {
		return fmt.Errorf("row/skipped-column metadata mismatch: %d/%d", len(event.Rows), len(event.SkippedColumns))
	}
	skippedAt := func(index int) []int {
		if index < len(event.SkippedColumns) {
			return event.SkippedColumns[index]
		}
		return nil
	}
	recordIndex := 0
	for index := 0; index < len(event.Rows); {
		var before map[string]any
		var after map[string]any
		if operation == "UPDATE" {
			before = rowObject(event.Rows[index], skippedAt(index), names)
			after = rowObject(event.Rows[index+1], skippedAt(index+1), names)
			index += 2
		} else if operation == "INSERT" {
			after = rowObject(event.Rows[index], skippedAt(index), names)
			index++
		} else {
			before = rowObject(event.Rows[index], skippedAt(index), names)
			index++
		}
		beforeJSON := ""
		afterJSON := ""
		if before != nil {
			beforeJSON, err = compactJSON(before)
			if err != nil {
				return fmt.Errorf("encode before image: %w", err)
			}
		}
		if after != nil {
			afterJSON, err = compactJSON(after)
			if err != nil {
				return fmt.Errorf("encode after image: %w", err)
			}
		}
		recordIndex++
		sqlTextValue := ""
		if !x.slim {
			sqlTextValue = pseudoSQL(operation, databaseName, tableName, before, after)
		}
		if err := x.emit(outputEvent{
			EventEpochUS:  epoch,
			RawEventType:  binlogEvent.Header.EventType.String(),
			Operation:     operation,
			DatabaseName:  databaseName,
			TableName:     tableName,
			ServerID:      binlogEvent.Header.ServerID,
			ThreadID:      x.currentThreadID,
			TransactionID: transactionID,
			GTID:          x.currentGTID,
			StartPosition: start,
			EndPosition:   end,
			RowIndex:      recordIndex,
			SQLKind:       "PSEUDO",
			SQLText:       sqlTextValue,
			BeforeJSON:    beforeJSON,
			AfterJSON:     afterJSON,
			ColumnsJSON:   columnsJSON,
			RowQuery:      rowQuery,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (x *extractor) handleEvent(
	binlogEvent *replication.BinlogEvent,
	overrideStart *uint32,
	overrideEnd *uint32,
) error {
	header := binlogEvent.Header
	if header.Timestamp > 0 {
		x.lastEpochUS = int64(header.Timestamp) * 1_000_000
	}
	switch event := binlogEvent.Event.(type) {
	case *replication.GTIDEvent:
		gset, err := event.GTIDNext()
		if err != nil {
			return fmt.Errorf("decode GTID: %w", err)
		}
		x.currentGTID = gset.String()
		x.currentTransaction = x.currentGTID
		if event.OriginalCommitTimestamp > 0 {
			x.transactionEpochUS = int64(event.OriginalCommitTimestamp)
		} else if header.Timestamp > 0 {
			x.transactionEpochUS = int64(header.Timestamp) * 1_000_000
		}
	case *replication.MariadbGTIDEvent:
		x.currentGTID = event.GTID.String()
		x.currentTransaction = x.currentGTID
		if header.Timestamp > 0 {
			x.transactionEpochUS = int64(header.Timestamp) * 1_000_000
		}
	case *replication.RowsQueryEvent:
		query, _ := sqlText(event.Query)
		x.currentRowQuery = query
	case *replication.MariadbAnnotateRowsEvent:
		query, _ := sqlText(event.Query)
		x.currentRowQuery = query
	case *replication.QueryEvent:
		start, end := eventPositions(header)
		if overrideStart != nil && overrideEnd != nil {
			start, end = *overrideStart, *overrideEnd
		}
		x.currentThreadID = event.SlaveProxyID
		query, encoded := sqlText(event.Query)
		if x.slim {
			encoded = ""
		}
		operation := classifyQuery(query)
		keyword := leadingKeyword(query)
		if keyword == "BEGIN" && x.currentTransaction == "" {
			x.fallbackTransaction(start)
		}
		transactionID := x.currentTransaction
		if transactionID == "" && operation != "DDL" {
			transactionID = x.fallbackTransaction(start)
		}
		if err := x.emit(outputEvent{
			EventEpochUS:    x.epochUS(header),
			RawEventType:    header.EventType.String(),
			Operation:       operation,
			DatabaseName:    byteText(event.Schema),
			ServerID:        header.ServerID,
			ThreadID:        event.SlaveProxyID,
			TransactionID:   transactionID,
			GTID:            x.currentGTID,
			StartPosition:   start,
			EndPosition:     end,
			ExecutionTimeMS: int64(event.ExecutionTime) * 1000,
			ErrorCode:       event.ErrorCode,
			SQLKind:         "ORIGINAL",
			SQLText:         query,
			SQLBytesBase64:  encoded,
		}); err != nil {
			return err
		}
		if keyword == "COMMIT" || keyword == "ROLLBACK" || operation == "DDL" {
			x.clearTransaction()
		}
	case *replication.RowsEvent:
		return x.handleRows(binlogEvent, event, overrideStart, overrideEnd)
	case *replication.XIDEvent:
		start, end := eventPositions(header)
		if overrideStart != nil && overrideEnd != nil {
			start, end = *overrideStart, *overrideEnd
		}
		transactionID := x.fallbackTransaction(start)
		if err := x.emit(outputEvent{
			EventEpochUS:  x.epochUS(header),
			RawEventType:  header.EventType.String(),
			Operation:     "TRANSACTION",
			ServerID:      header.ServerID,
			ThreadID:      x.currentThreadID,
			TransactionID: transactionID,
			GTID:          x.currentGTID,
			XID:           strconv.FormatUint(event.XID, 10),
			StartPosition: start,
			EndPosition:   end,
			SQLKind:       "BOUNDARY",
			SQLText:       "COMMIT /* XID " + strconv.FormatUint(event.XID, 10) + " */",
		}); err != nil {
			return err
		}
		x.clearTransaction()
	case *replication.TransactionPayloadEvent:
		start, end := eventPositions(header)
		for _, child := range event.Events {
			if err := x.handleEvent(child, &start, &end); err != nil {
				return err
			}
		}
	}
	return nil
}

func normalizeFormatDescriptionEvent(raw []byte) ([]byte, error) {
	if len(raw) < minFormatDescriptionEventSize {
		return nil, fmt.Errorf("FormatDescriptionEvent is too short: %d bytes", len(raw))
	}
	if raw[4] != formatDescriptionEventType {
		return nil, fmt.Errorf("first binlog event is type %d, want FormatDescriptionEvent", raw[4])
	}
	eventSize := binary.LittleEndian.Uint32(
		raw[binlogEventSizeOffset : binlogEventSizeOffset+4],
	)
	if uint32(len(raw)) != eventSize {
		return nil, fmt.Errorf(
			"FormatDescriptionEvent size mismatch: header=%d actual=%d",
			eventSize,
			len(raw),
		)
	}
	if len(raw) < minFormatDescriptionEventSize+formatDescriptionChecksumTailLen {
		return raw, nil
	}
	if raw[len(raw)-formatDescriptionChecksumTailLen] != binlogChecksumAlgorithmCRC32 {
		return raw, nil
	}

	expected := binary.LittleEndian.Uint32(raw[len(raw)-binlogChecksumLength:])
	direct := crc32.ChecksumIEEE(raw[:len(raw)-binlogChecksumLength])
	if direct == expected {
		return raw, nil
	}

	flags := binary.LittleEndian.Uint16(
		raw[binlogEventFlagsOffset : binlogEventFlagsOffset+2],
	)
	if flags&logEventBinlogInUseFlag == 0 {
		return nil, fmt.Errorf("FormatDescriptionEvent checksum mismatch")
	}

	canonical := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint16(
		canonical[binlogEventFlagsOffset:binlogEventFlagsOffset+2],
		flags&^logEventBinlogInUseFlag,
	)
	actual := crc32.ChecksumIEEE(canonical[:len(canonical)-binlogChecksumLength])
	if actual != expected {
		return nil, fmt.Errorf(
			"FormatDescriptionEvent checksum mismatch under direct and MySQL in-use flag verification",
		)
	}
	return canonical, nil
}

func prepareBinlogReader(reader io.Reader) (io.Reader, error) {
	header := make([]byte, binlogEventHeaderSize)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("read FormatDescriptionEvent header: %w", err)
	}
	eventSize := binary.LittleEndian.Uint32(
		header[binlogEventSizeOffset : binlogEventSizeOffset+4],
	)
	if eventSize < binlogEventHeaderSize || eventSize > maxFormatDescriptionEventSize {
		return nil, fmt.Errorf("invalid FormatDescriptionEvent size: %d", eventSize)
	}
	raw := make([]byte, eventSize)
	copy(raw, header)
	if _, err := io.ReadFull(reader, raw[binlogEventHeaderSize:]); err != nil {
		return nil, fmt.Errorf("read FormatDescriptionEvent body: %w", err)
	}
	normalized, err := normalizeFormatDescriptionEvent(raw)
	if err != nil {
		return nil, err
	}
	return io.MultiReader(bytes.NewReader(normalized), reader), nil
}

func (x *extractor) parseBinlog(reader io.Reader) error {
	x.resetStreamState()
	magic := make([]byte, 4)
	if _, err := io.ReadFull(reader, magic); err != nil {
		return fmt.Errorf("read MySQL binlog header: %w", err)
	}
	if !isRawBinlog(magic) {
		return errUnsupportedInput
	}
	prepared, err := prepareBinlogReader(reader)
	if err != nil {
		return fmt.Errorf("prepare MySQL binlog: %w", err)
	}
	parser := replication.NewBinlogParser()
	parser.SetFlavor(x.flavor)
	parser.SetVerifyChecksum(true)
	parser.SetParseTime(true)
	parser.SetTimestampStringLocation(time.UTC)
	parser.SetUseDecimal(true)
	parser.SetUseFloatWithTrailingZero(true)
	parser.SetRenderJSONAsMySQLText(true)
	parser.SetIgnoreJSONDecodeError(false)
	parser.SetPayloadDecoderConcurrency(2)
	if err := parser.ParseReader(prepared, func(event *replication.BinlogEvent) error {
		return x.handleEvent(event, nil, nil)
	}); err != nil {
		return fmt.Errorf("parse MySQL binlog: %w", err)
	}
	return nil
}

func isTarHeader(header []byte) bool {
	return len(header) >= 262 && string(header[257:262]) == "ustar"
}

func isRawBinlog(header []byte) bool {
	return len(header) >= 4 && header[0] == 0xfe && header[1] == 'b' && header[2] == 'i' && header[3] == 'n'
}

func isGzip(header []byte) bool {
	return len(header) >= 2 && header[0] == 0x1f && header[1] == 0x8b
}

func isZstd(header []byte) bool {
	return len(header) >= 4 && header[0] == 0x28 && header[1] == 0xb5 && header[2] == 0x2f && header[3] == 0xfd
}

func (x *extractor) parseTar(reader io.Reader) error {
	archive := tar.NewReader(reader)
	found := 0
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read tar header: %w", err)
		}
		if !header.FileInfo().Mode().IsRegular() || header.Size == 0 {
			continue
		}
		err = x.parseStream(io.LimitReader(archive, header.Size))
		if errors.Is(err, errUnsupportedInput) {
			continue
		}
		if err != nil {
			return fmt.Errorf("parse tar entry %q: %w", header.Name, err)
		}
		found++
	}
	if found == 0 {
		return errUnsupportedInput
	}
	return nil
}

func (x *extractor) parseStream(reader io.Reader) error {
	buffered := bufio.NewReaderSize(reader, 64*1024)
	header, _ := buffered.Peek(512)
	switch {
	case isRawBinlog(header):
		return x.parseBinlog(buffered)
	case isGzip(header):
		decompressor, err := gzip.NewReader(buffered)
		if err != nil {
			return fmt.Errorf("open gzip stream: %w", err)
		}
		defer decompressor.Close()
		return x.parseStream(decompressor)
	case isZstd(header):
		decompressor, err := zstd.NewReader(buffered, zstd.WithDecoderConcurrency(2))
		if err != nil {
			return fmt.Errorf("open zstd stream: %w", err)
		}
		defer decompressor.Close()
		return x.parseStream(decompressor)
	case isTarHeader(header):
		return x.parseTar(buffered)
	default:
		return errUnsupportedInput
	}
}

func (x *extractor) parseZip(readerAt io.ReaderAt, size int64) error {
	archive, err := zip.NewReader(readerAt, size)
	if err != nil {
		return fmt.Errorf("open zip archive: %w", err)
	}
	found := 0
	for _, entry := range archive.File {
		if entry.FileInfo().IsDir() || entry.UncompressedSize64 == 0 {
			continue
		}
		reader, err := entry.Open()
		if err != nil {
			return fmt.Errorf("open zip entry %q: %w", entry.Name, err)
		}
		parseErr := x.parseStream(reader)
		closeErr := reader.Close()
		if errors.Is(parseErr, errUnsupportedInput) {
			continue
		}
		if parseErr != nil {
			return fmt.Errorf("parse zip entry %q: %w", entry.Name, parseErr)
		}
		if closeErr != nil {
			return fmt.Errorf("close zip entry %q: %w", entry.Name, closeErr)
		}
		found++
	}
	if found == 0 {
		return errUnsupportedInput
	}
	return nil
}

func (x *extractor) parsePath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open input: %w", err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat input: %w", err)
	}
	if !stat.Mode().IsRegular() {
		return errors.New("input must be a regular file")
	}
	header := make([]byte, 4)
	count, readErr := io.ReadFull(file, header)
	if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		return fmt.Errorf("read input header: %w", readErr)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind input: %w", err)
	}
	if count == 4 && header[0] == 'P' && header[1] == 'K' && header[2] == 0x03 && header[3] == 0x04 {
		return x.parseZip(file, stat.Size())
	}
	return x.parseStream(file)
}

func run(args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer) int {
	flags := flag.NewFlagSet("binlog-parser", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "path to a MySQL binlog or compressed archive")
	sourceFileID := flags.String("source-file-id", "", "stable source-file identifier")
	flavor := flags.String("flavor", "mysql", "binlog flavor: mysql or mariadb")
	outputDir := flags.String("output-dir", "", "publish atomic NDJSON chunks and emit manifests")
	chunkMaxLines := flags.Int("chunk-max-lines", 200_000, "maximum records per NDJSON chunk")
	chunkMaxBytes := flags.Int64("chunk-max-bytes", 384*1024*1024, "maximum bytes per NDJSON chunk")
	slim := flags.Bool("slim", false, "row-store output: omit pseudo SQL, column metadata, base64 SQL and transaction boundaries; cut row_query to 65536 code points")
	checksumStdin := flags.Bool(
		"checksum-stdin",
		false,
		"read stdin and emit size, SHA-256 and Alibaba-compatible CRC64 as JSON",
	)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *checksumStdin {
		result, err := checksumReader(stdin)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := json.NewEncoder(stdout).Encode(result); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}
	if strings.TrimSpace(*input) == "" || strings.TrimSpace(*sourceFileID) == "" {
		fmt.Fprintln(stderr, "--input and --source-file-id are required")
		return 2
	}
	normalizedFlavor := strings.ToLower(strings.TrimSpace(*flavor))
	if normalizedFlavor != "mysql" && normalizedFlavor != "mariadb" {
		fmt.Fprintln(stderr, "--flavor must be mysql or mariadb")
		return 2
	}
	var output *chunkedOutput
	writer := stdout
	if strings.TrimSpace(*outputDir) != "" {
		var err error
		output, err = newChunkedOutput(
			*outputDir,
			*sourceFileID,
			*chunkMaxLines,
			*chunkMaxBytes,
			stdout,
			stdin,
		)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		writer = output
	}
	extractor := newExtractor(writer, *sourceFileID, normalizedFlavor)
	extractor.slim = *slim
	if output != nil {
		extractor.afterEncode = output.recordComplete
	}
	if err := extractor.parsePath(*input); err != nil {
		if output != nil {
			output.Abort()
		}
		fmt.Fprintln(stderr, err)
		return 1
	}
	if output != nil {
		if err := output.Close(); err != nil {
			output.Abort()
			fmt.Fprintln(stderr, err)
			return 1
		}
	}
	fmt.Fprintf(stderr, "parsed %d audit records\n", extractor.emitted)
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
