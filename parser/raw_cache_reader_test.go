package main

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

func testRawCache(t *testing.T, codec byte, sourceID string, frames ...[]byte) []byte {
	t.Helper()
	identity, err := hex.DecodeString(sourceID)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, frame := range frames {
		total += len(frame)
	}
	result := bytes.NewBuffer(nil)
	result.Write(rawCacheMagic)
	result.WriteByte(codec)
	if err := binary.Write(result, binary.LittleEndian, uint64(total)); err != nil {
		t.Fatal(err)
	}
	result.Write(identity)
	if err := binary.Write(result, binary.LittleEndian, uint32(rawCacheMinFrame)); err != nil {
		t.Fatal(err)
	}
	full := sha256.New()
	for _, raw := range frames {
		var compressed []byte
		switch codec {
		case 1:
			encoder, openErr := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
			if openErr != nil {
				t.Fatal(openErr)
			}
			compressed = encoder.EncodeAll(raw, nil)
			encoder.Close()
		case 2:
			buffer := bytes.NewBuffer(nil)
			writer := lz4.NewWriter(buffer)
			if _, err := writer.Write(raw); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			compressed = buffer.Bytes()
		default:
			t.Fatalf("unsupported test codec %d", codec)
		}
		result.WriteString("FRM1")
		if err := binary.Write(result, binary.LittleEndian, uint32(len(raw))); err != nil {
			t.Fatal(err)
		}
		if err := binary.Write(result, binary.LittleEndian, uint32(len(compressed))); err != nil {
			t.Fatal(err)
		}
		checksum := sha256.Sum256(raw)
		result.Write(checksum[:])
		result.Write(compressed)
		full.Write(raw)
	}
	result.WriteString("END1")
	if err := binary.Write(result, binary.LittleEndian, uint64(total)); err != nil {
		t.Fatal(err)
	}
	result.Write(full.Sum(nil))
	return result.Bytes()
}

func TestRawCacheReaderValidatesAndReplaysBothCodecs(t *testing.T) {
	const sourceID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	want := []byte("first-frame-second-frame")
	for _, codec := range []byte{1, 2} {
		cache := testRawCache(t, codec, sourceID, []byte("first-frame-"), []byte("second-frame"))
		path := filepath.Join(t.TempDir(), "source.rawcache")
		if err := os.WriteFile(path, cache, 0o600); err != nil {
			t.Fatal(err)
		}
		reader, err := openRawCacheReaderAt(path, sourceID, int64(len(want)))
		if err != nil {
			t.Fatalf("codec %d rejected: %v", codec, err)
		}
		got, err := io.ReadAll(io.NewSectionReader(reader, 0, reader.size))
		closeErr := reader.Close()
		if err != nil || closeErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("codec %d replay changed: got=%q err=%v close=%v", codec, got, err, closeErr)
		}
	}
}

func testRawArchive(t *testing.T, format string, raw []byte) []byte {
	t.Helper()
	buffer := bytes.NewBuffer(nil)
	switch format {
	case "raw":
		return append([]byte(nil), raw...)
	case "gzip":
		writer := gzip.NewWriter(buffer)
		if _, err := writer.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	case "zstd":
		writer, err := zstd.NewWriter(buffer, zstd.WithEncoderConcurrency(1))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	case "tar":
		writer := tar.NewWriter(buffer)
		if err := writer.WriteHeader(&tar.Header{
			Name: "source.binlog", Mode: 0o600, Size: int64(len(raw)),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := writer.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	case "zip":
		writer := zip.NewWriter(buffer)
		entry, err := writer.Create("source.binlog")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write(raw); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("unsupported archive format %q", format)
	}
	return buffer.Bytes()
}

func TestRawCacheParserReplaysEverySupportedSourceContainer(t *testing.T) {
	const sourceID = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
	payload := []byte("deliberately-not-a-binlog")
	for _, format := range []string{"raw", "gzip", "zstd", "tar", "zip"} {
		archive := testRawArchive(t, format, payload)
		for _, codec := range []byte{1, 2} {
			cache := testRawCache(t, codec, sourceID, archive)
			path := filepath.Join(t.TempDir(), format+".rawcache")
			if err := os.WriteFile(path, cache, 0o600); err != nil {
				t.Fatal(err)
			}
			extractor := newExtractor(io.Discard, sourceID, "mysql")
			err := extractor.parseRawCachePath(path, sourceID, int64(len(archive)))
			if err == nil || !strings.Contains(err.Error(), errUnsupportedInput.Error()) {
				t.Fatalf("format=%s codec=%d replay changed parser input: %v", format, codec, err)
			}
		}
	}
}

func TestRawCacheReaderRejectsMalformedProtocolBoundaries(t *testing.T) {
	const sourceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	raw := []byte("source")
	valid := testRawCache(t, 1, sourceID, raw)
	footerAt := len(valid) - rawCacheRecordSize
	cases := map[string]struct {
		mutate func([]byte) []byte
		want   string
	}{
		"truncated-header": {
			mutate: func(value []byte) []byte { return value[:rawCacheHeaderSize-1] },
			want:   "header",
		},
		"unknown-codec": {
			mutate: func(value []byte) []byte { value[8] = 9; return value },
			want:   "codec",
		},
		"wrong-declared-size": {
			mutate: func(value []byte) []byte {
				binary.LittleEndian.PutUint64(value[9:17], uint64(len(raw)+1))
				return value
			},
			want: "expected size",
		},
		"truncated-frame-record": {
			mutate: func(value []byte) []byte { return value[:rawCacheHeaderSize+20] },
			want:   "record",
		},
		"zero-frame-size": {
			mutate: func(value []byte) []byte {
				binary.LittleEndian.PutUint32(value[rawCacheHeaderSize+4:rawCacheHeaderSize+8], 0)
				return value
			},
			want: "declared size budget",
		},
		"wrong-footer-count": {
			mutate: func(value []byte) []byte {
				binary.LittleEndian.PutUint64(value[footerAt+4:footerAt+12], uint64(len(raw)+1))
				return value
			},
			want: "final size",
		},
		"wrong-footer-sha": {
			mutate: func(value []byte) []byte { value[len(value)-1] ^= 0xff; return value },
			want:   "final SHA256",
		},
		"truncated-footer": {
			mutate: func(value []byte) []byte { return value[:len(value)-1] },
			want:   "record",
		},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			value := test.mutate(append([]byte(nil), valid...))
			path := filepath.Join(t.TempDir(), "source.rawcache")
			if err := os.WriteFile(path, value, 0o600); err != nil {
				t.Fatal(err)
			}
			reader, err := openRawCacheReaderAt(path, sourceID, int64(len(raw)))
			if reader != nil {
				reader.Close()
			}
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("malformed cache was not rejected with %q: %v", test.want, err)
			}
		})
	}
}

func TestRawCacheReaderRejectsIdentityCorruptionAndTrailingData(t *testing.T) {
	const sourceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cache := testRawCache(t, 1, sourceID, []byte("source"))
	path := filepath.Join(t.TempDir(), "source.rawcache")
	if err := os.WriteFile(path, cache, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openRawCacheReaderAt(path, strings.Repeat("b", 64), 6); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("identity mismatch was not rejected: %v", err)
	}
	corrupt := append([]byte(nil), cache...)
	corrupt[rawCacheHeaderSize+rawCacheRecordSize] ^= 0xff
	if err := os.WriteFile(path, corrupt, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openRawCacheReaderAt(path, sourceID, 6); err == nil {
		t.Fatal("corrupt frame was not rejected")
	}
	trailing := append(append([]byte(nil), cache...), 0)
	if err := os.WriteFile(path, trailing, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := openRawCacheReaderAt(path, sourceID, 6); err == nil || !strings.Contains(err.Error(), "trailing") {
		t.Fatalf("trailing data was not rejected: %v", err)
	}
}
