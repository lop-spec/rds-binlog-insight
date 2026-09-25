package main

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/replication"
)

func testFrame(kind byte, start uint32, body []byte) []byte {
	size := uint32(binlogEventHeaderSize + len(body))
	raw := make([]byte, size)
	raw[4] = kind
	binary.LittleEndian.PutUint32(raw[binlogEventSizeOffset:binlogEventSizeOffset+4], size)
	binary.LittleEndian.PutUint32(raw[13:17], start+size)
	copy(raw[binlogEventHeaderSize:], body)
	return raw
}

func TestNormalizeValuePreservesArbitraryBytes(t *testing.T) {
	raw := []byte{0x00, 0xff, 0xfe}
	got, ok := normalizeValue(raw).(map[string]any)
	if !ok {
		t.Fatalf("normalized bytes have type %T", normalizeValue(raw))
	}
	if got["$binary_base64"] != base64.StdEncoding.EncodeToString(raw) || got["$length"] != len(raw) {
		t.Fatalf("normalized bytes changed: %#v", got)
	}
	if literal := pseudoLiteral(got); literal != "FROM_BASE64('AP/+')" {
		t.Fatalf("binary pseudo literal is not lossless: %s", literal)
	}
	fromString, ok := normalizeColumnValue(string(raw), 0, map[int]bool{}).(map[string]any)
	if !ok || fromString["$binary_base64"] != "AP/+" {
		t.Fatalf("invalid UTF-8 Go string was not preserved as bytes: %#v", fromString)
	}
	emptyBinary, ok := normalizeColumnValue("", 3, map[int]bool{3: true}).(map[string]any)
	if !ok || emptyBinary["$binary_base64"] != "" || emptyBinary["$length"] != 0 {
		t.Fatalf("empty VARBINARY lost binary type identity: %#v", emptyBinary)
	}
}

func TestSchemaVersionIDMatchesFrozenLegacyIdentity(t *testing.T) {
	columnsJSON := `[{"index":0,"name":"id","type_id":8,"metadata":0,"nullable_known":true,"nullable":false,"primary_key":true}]`
	// The exact algorithm is independently recovered from the prior native
	// binary: SHA-256(lower(db) NUL lower(table) NUL columns_json).
	const want = "c0cd6c132a626df93b85e4a1d7e1fa9bb9170c13e96a0fbb4460ff36a19cf222"
	if got := schemaVersionID("Fixture", "Rows_ABI", columnsJSON); got != want {
		t.Fatalf("schema identity changed: got %s want %s", got, want)
	}
}

func TestRawCacheFlagsArePresenceBoundAndRequiredTogether(t *testing.T) {
	const sourceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	base := []string{"--input", "missing", "--source-file-id", sourceID}
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "source only",
			args: []string{"--raw-cache-source-id", sourceID},
			want: "required together",
		},
		{
			name: "size only",
			args: []string{"--raw-cache-expected-size", "0"},
			want: "required together",
		},
		{
			name: "explicit negative size only",
			args: []string{"--raw-cache-expected-size", "-2"},
			want: "required together",
		},
		{
			name: "explicit negative size with identity",
			args: []string{
				"--raw-cache-source-id", sourceID,
				"--raw-cache-expected-size", "-2",
			},
			want: "must be non-negative",
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			args := append(append([]string(nil), base...), test.args...)
			if status := run(args, bytes.NewReader(nil), &stdout, &stderr); status != 2 {
				t.Fatalf("raw-cache flag error status = %d, stderr=%q", status, stderr.String())
			}
			if stdout.Len() != 0 || !strings.Contains(stderr.String(), test.want) {
				t.Fatalf("raw-cache flag error changed: stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestReadBinlogEventRequiresCompleteFrameAndPosition(t *testing.T) {
	raw := testFrame(2, 4, []byte("body"))
	got, eof, err := readBinlogEvent(bytes.NewReader(raw), 4)
	if err != nil || eof || !bytes.Equal(got, raw) {
		t.Fatalf("complete frame rejected: eof=%v err=%v", eof, err)
	}
	if _, _, err := readBinlogEvent(bytes.NewReader(raw[:7]), 4); err == nil || !strings.Contains(err.Error(), "partial binlog event header") {
		t.Fatalf("partial header was not rejected: %v", err)
	}
	if _, _, err := readBinlogEvent(bytes.NewReader(raw[:len(raw)-1]), 4); err == nil || !strings.Contains(err.Error(), "partial binlog event body") {
		t.Fatalf("partial body was not rejected: %v", err)
	}
	badPosition := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(badPosition[13:17], 999)
	if _, _, err := readBinlogEvent(bytes.NewReader(badPosition), 4); err == nil || !strings.Contains(err.Error(), "position mismatch") {
		t.Fatalf("bad position was not rejected: %v", err)
	}
	badSize := append([]byte(nil), raw...)
	binary.LittleEndian.PutUint32(badSize[binlogEventSizeOffset:binlogEventSizeOffset+4], 18)
	if _, _, err := readBinlogEvent(bytes.NewReader(badSize), 4); err == nil || !strings.Contains(err.Error(), "invalid binlog event size") {
		t.Fatalf("bad size was not rejected: %v", err)
	}
}

func TestInspectDecodedEventRequiresObservedTableMap(t *testing.T) {
	coverage := parserCoverage{tableMaps: make(map[uint64]struct{})}
	rows := &replication.RowsEvent{TableID: 87, Table: &replication.TableMapEvent{TableID: 87}}
	event := &replication.BinlogEvent{Header: &replication.EventHeader{LogPos: 100}, Event: rows}
	if err := inspectDecodedEvent(event, &coverage); err == nil || !strings.Contains(err.Error(), "unseen TableMap") {
		t.Fatalf("unseen table map was not rejected: %v", err)
	}
	mapping := &replication.BinlogEvent{Header: &replication.EventHeader{LogPos: 80}, Event: rows.Table}
	if err := inspectDecodedEvent(mapping, &coverage); err != nil {
		t.Fatal(err)
	}
	if err := inspectDecodedEvent(event, &coverage); err != nil {
		t.Fatalf("observed table map was rejected: %v", err)
	}
}
