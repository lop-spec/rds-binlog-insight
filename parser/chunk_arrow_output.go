package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// chunkedArrowOutput publishes one bounded Arrow IPC file at a time. A manifest
// is emitted only after the file and containing directory are durable. The ACK
// transfers ownership to the collector's bounded outstanding set; it does not
// permit overwriting or reusing the deterministic chunk path.
type chunkedArrowOutput struct {
	outputDir       string
	sourceFileID    string
	maxLines        int
	maxBytes        int64
	manifestEncoder *json.Encoder
	ackReader       *bufio.Reader
	index           int
	current         *atomicArrowOutput
	publishedPath   string
	rows            int
	decodedBytes    int64
	closed          bool
}

func newChunkedArrowOutput(
	outputDir string,
	sourceFileID string,
	maxLines int,
	maxBytes int64,
	manifestWriter io.Writer,
	ackReader io.Reader,
) (*chunkedArrowOutput, error) {
	absoluteDir, safeSourceFileID, err := validateChunkDestination(outputDir, sourceFileID)
	if err != nil {
		return nil, err
	}
	if maxLines <= 0 {
		return nil, errors.New("chunk row limit must be positive")
	}
	if maxBytes < minimumArrowByteLimit || maxBytes > maximumArrowOutputBytes {
		return nil, fmt.Errorf(
			"Arrow chunk bytes must be between %d and %d",
			minimumArrowByteLimit,
			maximumArrowOutputBytes,
		)
	}
	if manifestWriter == nil || ackReader == nil {
		return nil, errors.New("chunk manifest writer and ACK reader are required")
	}
	return &chunkedArrowOutput{
		outputDir:       absoluteDir,
		sourceFileID:    safeSourceFileID,
		maxLines:        maxLines,
		maxBytes:        maxBytes,
		manifestEncoder: json.NewEncoder(manifestWriter),
		ackReader:       bufio.NewReader(ackReader),
	}, nil
}

func (c *chunkedArrowOutput) openChunk() error {
	if c.closed {
		return errors.New("Arrow chunk output is closed")
	}
	if c.current != nil {
		return nil
	}
	batchRows := defaultArrowBatchRows
	if c.maxLines < batchRows {
		batchRows = c.maxLines
	}
	batchBytes := defaultArrowBatchBytes
	if c.maxBytes < batchBytes {
		batchBytes = c.maxBytes
	}
	finalPath := filepath.Join(
		c.outputDir,
		fmt.Sprintf("%s-%06d.arrow", c.sourceFileID, c.index),
	)
	output, err := newAtomicArrowOutput(finalPath, batchRows, batchBytes, c.maxBytes)
	if err != nil {
		return fmt.Errorf("create Arrow chunk: %w", err)
	}
	c.current = output
	return nil
}

func (c *chunkedArrowOutput) Encode(event outputEvent) error {
	if c.closed {
		return errors.New("Arrow chunk output is closed")
	}
	recordBytes := arrowEventBytes(event)
	if recordBytes > c.maxBytes {
		return fmt.Errorf(
			"Arrow record estimate %d exceeds %d-byte chunk limit",
			recordBytes,
			c.maxBytes,
		)
	}
	if c.rows > 0 && (c.rows >= c.maxLines || recordBytes > c.maxBytes-c.decodedBytes) {
		if err := c.publish(); err != nil {
			return err
		}
	}
	if err := c.openChunk(); err != nil {
		return err
	}
	if err := c.current.Encode(event); err != nil {
		return err
	}
	c.rows++
	c.decodedBytes += recordBytes
	if c.rows >= c.maxLines || c.decodedBytes >= c.maxBytes {
		return c.publish()
	}
	return nil
}

func decodeArrowChunkACK(raw string, sequence int) error {
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	var ack arrowChunkACK
	if err := decoder.Decode(&ack); err != nil {
		return fmt.Errorf("decode Arrow chunk ACK: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("Arrow chunk ACK contains trailing data")
	}
	if ack.Protocol != arrowChunkACKProtocol || ack.Sequence != sequence || ack.Status != "ok" {
		return fmt.Errorf("unexpected Arrow chunk ACK for sequence %d", sequence)
	}
	return nil
}

func removeFailedPublishedChunk(path string) {
	if path == "" {
		return
	}
	_ = os.Remove(path)
	_ = syncArrowDirectory(filepath.Dir(path))
}

func (c *chunkedArrowOutput) publish() error {
	if c.rows == 0 {
		return nil
	}
	if c.current == nil {
		return errors.New("Arrow chunk state is incomplete")
	}
	published := c.current.finalPath
	if err := c.current.Close(); err != nil {
		return fmt.Errorf("publish Arrow chunk: %w", err)
	}
	c.publishedPath = published
	stat, err := os.Stat(published)
	if err != nil {
		removeFailedPublishedChunk(published)
		return fmt.Errorf("stat published Arrow chunk: %w", err)
	}
	manifest := chunkManifest{
		Protocol:     chunkManifestProtocol,
		Format:       arrowChunkFormat,
		Sequence:     c.index,
		Path:         published,
		Rows:         c.rows,
		Bytes:        stat.Size(),
		DecodedBytes: c.decodedBytes,
	}
	if err := c.manifestEncoder.Encode(manifest); err != nil {
		removeFailedPublishedChunk(published)
		return fmt.Errorf("write Arrow chunk manifest: %w", err)
	}
	rawACK, err := c.ackReader.ReadString('\n')
	if err != nil {
		removeFailedPublishedChunk(published)
		return fmt.Errorf("wait for Arrow chunk ACK: %w", err)
	}
	if err := decodeArrowChunkACK(rawACK, c.index); err != nil {
		removeFailedPublishedChunk(published)
		return err
	}
	c.index++
	c.current = nil
	c.publishedPath = ""
	c.rows = 0
	c.decodedBytes = 0
	return nil
}

func (c *chunkedArrowOutput) Close() error {
	if c.closed {
		return nil
	}
	if err := c.publish(); err != nil {
		return err
	}
	c.closed = true
	return nil
}

func (c *chunkedArrowOutput) Abort() {
	if c.closed {
		return
	}
	c.closed = true
	if c.current != nil {
		c.current.Abort()
		c.current = nil
	}
	removeFailedPublishedChunk(c.publishedPath)
	c.publishedPath = ""
}
