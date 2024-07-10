package wal

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type WAL struct {
	directory string
	mu        sync.Mutex
}

func NewWAL(directory string) (*WAL, error) {
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	return &WAL{
		directory: directory,
	}, nil
}

func (w *WAL) Append(topic string, partitionID int, message []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	fileName := filepath.Join(w.directory, fmt.Sprintf("%s-%d.log", topic, partitionID))
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer file.Close()

	if _, err := file.Write(append(message, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}
	return nil
}

func (w *WAL) Read(topic string, partitionID int) ([][]byte, error) {
	fileName := filepath.Join(w.directory, fmt.Sprintf("%s-%d.log", topic, partitionID))
	file, err := os.Open(fileName)
	if err != nil {
		if os.IsNotExist(err) {
			return [][]byte{}, nil
		}
	}
	return nil, fmt.Errorf("failed to open WAL file: %v", err)

	defer file.Close()

	var messages [][]byte
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		messages = append(messages, scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL: %v", err)
	}

	return messages, nil
}
