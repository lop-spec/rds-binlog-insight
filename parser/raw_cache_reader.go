package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const (
	rawCacheHeaderSize = 8 + 1 + 8 + 32 + 4
	rawCacheRecordSize = 4 + 4 + 4 + 32
	rawCacheMinFrame   = 64 * 1024
	rawCacheMaxFrame   = 16 * 1024 * 1024
)

var rawCacheMagic = []byte("RDSRAW1\n")

type rawCacheFrame struct {
	rawOffset        int64
	rawSize          uint32
	compressedOffset int64
	compressedSize   uint32
	checksum         [sha256.Size]byte
}

type rawCacheReaderAt struct {
	file     *os.File
	codec    byte
	size     int64
	frames   []rawCacheFrame
	decoder  *zstd.Decoder
	mu       sync.Mutex
	cacheAt  int
	cacheRaw []byte
}

func parseRawCacheIdentity(value string) ([sha256.Size]byte, error) {
	var identity [sha256.Size]byte
	if len(value) != sha256.Size*2 {
		return identity, errors.New("raw cache source identity must be a lowercase SHA256")
	}
	for _, character := range value {
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f')) {
			return identity, errors.New("raw cache source identity must be a lowercase SHA256")
		}
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return identity, errors.New("raw cache source identity must be a lowercase SHA256")
	}
	copy(identity[:], decoded)
	return identity, nil
}

func openRawCacheReaderAt(path, sourceID string, expectedSize int64) (*rawCacheReaderAt, error) {
	identity, err := parseRawCacheIdentity(sourceID)
	if err != nil {
		return nil, err
	}
	if expectedSize < 0 {
		return nil, errors.New("raw cache expected size must be non-negative")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open raw cache: %w", err)
	}
	fail := func(cause error) (*rawCacheReaderAt, error) {
		file.Close()
		return nil, cause
	}
	stat, err := file.Stat()
	if err != nil {
		return fail(fmt.Errorf("stat raw cache: %w", err))
	}
	if !stat.Mode().IsRegular() {
		return fail(errors.New("raw cache input must be a regular file"))
	}
	header := make([]byte, rawCacheHeaderSize)
	if _, err := io.ReadFull(file, header); err != nil {
		return fail(fmt.Errorf("raw cache header is truncated: %w", err))
	}
	if !bytes.Equal(header[:8], rawCacheMagic) {
		return fail(errors.New("raw cache magic is invalid"))
	}
	codec := header[8]
	if codec != 1 && codec != 2 {
		return fail(errors.New("raw cache codec is unsupported"))
	}
	declaredSize := binary.LittleEndian.Uint64(header[9:17])
	if declaredSize > uint64(^uint64(0)>>1) || int64(declaredSize) != expectedSize {
		return fail(errors.New("raw cache expected size does not match"))
	}
	if !bytes.Equal(header[17:49], identity[:]) {
		return fail(errors.New("raw cache source identity does not match"))
	}
	frameBytes := binary.LittleEndian.Uint32(header[49:53])
	if frameBytes < rawCacheMinFrame || frameBytes > rawCacheMaxFrame {
		return fail(errors.New("raw cache frame size is outside the protocol bounds"))
	}

	frames := make([]rawCacheFrame, 0)
	physicalOffset := int64(rawCacheHeaderSize)
	rawOffset := int64(0)
	var finalChecksum [sha256.Size]byte
	for {
		if physicalOffset+4 > stat.Size() {
			return fail(errors.New("raw cache is incomplete; complete footer is required"))
		}
		record := make([]byte, rawCacheRecordSize)
		if _, err := file.ReadAt(record, physicalOffset); err != nil {
			return fail(fmt.Errorf("read raw cache record: %w", err))
		}
		switch string(record[:4]) {
		case "END1":
			count := binary.LittleEndian.Uint64(record[4:12])
			if count > uint64(^uint64(0)>>1) || int64(count) != rawOffset || rawOffset != expectedSize {
				return fail(errors.New("raw cache final size does not match"))
			}
			copy(finalChecksum[:], record[12:44])
			physicalOffset += rawCacheRecordSize
			if physicalOffset != stat.Size() {
				return fail(errors.New("raw cache has trailing data after its footer"))
			}
			reader := &rawCacheReaderAt{
				file: file, codec: codec, size: expectedSize, frames: frames, cacheAt: -1,
			}
			if codec == 1 {
				reader.decoder, err = zstd.NewReader(
					nil,
					zstd.WithDecoderConcurrency(1),
					zstd.WithDecoderMaxMemory(rawCacheMaxFrame),
					zstd.WithDecoderMaxWindow(rawCacheMaxFrame),
					zstd.WithDecodeAllCapLimit(true),
				)
				if err != nil {
					return fail(fmt.Errorf("open raw cache zstd decoder: %w", err))
				}
			}
			digest := sha256.New()
			for index := range reader.frames {
				raw, decodeErr := reader.decodeFrame(index)
				if decodeErr != nil {
					reader.Close()
					return nil, decodeErr
				}
				if _, writeErr := digest.Write(raw); writeErr != nil {
					reader.Close()
					return nil, writeErr
				}
			}
			if !bytes.Equal(digest.Sum(nil), finalChecksum[:]) {
				reader.Close()
				return nil, errors.New("raw cache final SHA256 does not match")
			}
			reader.cacheAt = -1
			reader.cacheRaw = nil
			return reader, nil
		case "FRM1":
			rawSize := binary.LittleEndian.Uint32(record[4:8])
			compressedSize := binary.LittleEndian.Uint32(record[8:12])
			if rawSize == 0 || rawSize > frameBytes || compressedSize == 0 ||
				uint64(compressedSize) > uint64(frameBytes)*2+65536 ||
				rawOffset > expectedSize-int64(rawSize) {
				return fail(errors.New("raw cache frame exceeds the declared size budget"))
			}
			payloadOffset := physicalOffset + rawCacheRecordSize
			if payloadOffset > stat.Size()-int64(compressedSize) {
				return fail(errors.New("raw cache frame payload is truncated"))
			}
			frame := rawCacheFrame{
				rawOffset: rawOffset, rawSize: rawSize,
				compressedOffset: payloadOffset, compressedSize: compressedSize,
			}
			copy(frame.checksum[:], record[12:44])
			frames = append(frames, frame)
			rawOffset += int64(rawSize)
			physicalOffset = payloadOffset + int64(compressedSize)
		default:
			return fail(errors.New("raw cache frame marker is corrupt"))
		}
	}
}

func (reader *rawCacheReaderAt) decodeFrame(index int) ([]byte, error) {
	frame := reader.frames[index]
	compressed := make([]byte, int(frame.compressedSize))
	if _, err := reader.file.ReadAt(compressed, frame.compressedOffset); err != nil {
		return nil, fmt.Errorf("read raw cache frame: %w", err)
	}
	var raw []byte
	var err error
	switch reader.codec {
	case 1:
		raw, err = reader.decoder.DecodeAll(compressed, make([]byte, 0, int(frame.rawSize)))
	case 2:
		raw, err = io.ReadAll(io.LimitReader(lz4.NewReader(bytes.NewReader(compressed)), int64(frame.rawSize)+1))
	default:
		return nil, errors.New("raw cache codec is unsupported")
	}
	if err != nil {
		return nil, fmt.Errorf("decompress raw cache frame: %w", err)
	}
	if len(raw) != int(frame.rawSize) || sha256.Sum256(raw) != frame.checksum {
		return nil, errors.New("raw cache frame size/SHA256 does not match")
	}
	return raw, nil
}

func (reader *rawCacheReaderAt) ReadAt(destination []byte, offset int64) (int, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if offset < 0 {
		return 0, errors.New("raw cache read offset is negative")
	}
	if len(destination) == 0 {
		return 0, nil
	}
	if offset >= reader.size {
		return 0, io.EOF
	}
	read := 0
	for read < len(destination) && offset < reader.size {
		index := sort.Search(len(reader.frames), func(index int) bool {
			frame := reader.frames[index]
			return frame.rawOffset+int64(frame.rawSize) > offset
		})
		if index == len(reader.frames) || offset < reader.frames[index].rawOffset {
			return read, errors.New("raw cache frame index has a gap")
		}
		if reader.cacheAt != index {
			raw, err := reader.decodeFrame(index)
			if err != nil {
				return read, err
			}
			reader.cacheAt = index
			reader.cacheRaw = raw
		}
		inside := int(offset - reader.frames[index].rawOffset)
		count := copy(destination[read:], reader.cacheRaw[inside:])
		read += count
		offset += int64(count)
	}
	if read < len(destination) {
		return read, io.EOF
	}
	return read, nil
}

func (reader *rawCacheReaderAt) Close() error {
	if reader.decoder != nil {
		reader.decoder.Close()
	}
	return reader.file.Close()
}

func (x *extractor) parseRawCachePath(path, sourceID string, expectedSize int64) error {
	reader, err := openRawCacheReaderAt(path, sourceID, expectedSize)
	if err != nil {
		return err
	}
	defer reader.Close()
	section := io.NewSectionReader(reader, 0, reader.size)
	header := make([]byte, 4)
	count, err := section.ReadAt(header, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("read raw cache source header: %w", err)
	}
	if count == 4 && header[0] == 'P' && header[1] == 'K' && header[2] == 0x03 && header[3] == 0x04 {
		return x.parseZip(reader, reader.size)
	}
	return x.parseStream(section)
}
