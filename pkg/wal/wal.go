package wal

import (
	"fmt"
	"os"
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
