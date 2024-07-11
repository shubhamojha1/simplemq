package broker

import (
	"fmt"
	"sync"

	"github.com/shubhamojha1/simplemq/pkg/wal"
	"github.com/shubhamojha1/simplemq/pkg/zookeeper_client"
)

type Broker struct {
	ID           string
	zkClient     *zookeeper_client.ZookeeperClient
	wal          *wal.WAL
	topics       map[string][]int
	partitions   map[int]*Partition
	consumers    map[string]*Consumer
	producerLock sync.RWMutex
	consumerLock sync.RWMutex
	shutdownCh   chan struct{}
}

type Partition struct {
	ID     int
	Topic  string
	Leader string
}

type Consumer struct {
	ID     string
	Topics []string
}

func NewBroker(id string, zkClient *zookeeper_client.ZookeeperClient, wal *wal.WAL) *Broker {
	return &Broker{
		ID:         id,
		zkClient:   zkClient,
		wal:        wal,
		topics:     make(map[string][]int),
		partitions: make(map[int]*Partition),
		consumers:  make(map[string]*Consumer),
		shutdownCh: make(chan struct{}),
	}
}

func (b *Broker) Start() error {
	// Write to WAL before registering
	err := b.zkClient.WriteAheadLog([]byte(fmt.Sprintf("REGISTER_BROKER:%s", b.ID)))
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	// Register the broker to Zookeeper
	err = b.zkClient.RegisterBroker(b.ID)
	if err != nil {
		return fmt.Errorf("failed to register broker with Zookeeper: %v", err)
	}

	// replayWal() -> important to ensure data consistency, durability, persistence
	// helpful in case of -> recovery from crashes, atomicity and consistency,

	return nil
}

func (b *Broker) Stop() error {
	close(b.shutdownCh)
	// cleanup operations, unregister from Zookeeper, close connections, etc.
	return nil
}

func (b *Broker) ProduceMessage(topic string, message []byte) error {
	b.producerLock.RLock()
	defer b.producerLock.RUnlock()

	partitions, exists := b.topics[topic]
}
