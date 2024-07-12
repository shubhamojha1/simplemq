package broker

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

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
	err = b.replayWal()
	if err != nil {
		log.Printf("Error replaying WAL: %v", err)
	}

	// load topics and partitions
	err = b.loadTopicsAndPartitions()
	if err != nil {
		return fmt.Errorf("failed to load topics and partitions: %v", err)
	}

	go b.heartbeat()
	go b.monitorPartitions()

	log.Printf("Broker %s started successfully", b.ID)
	return nil
}

func (b *Broker) Stop() error {
	close(b.shutdownCh)
	// cleanup operations, unregister from Zookeeper, close connections, etc.
	return nil
}

func (b *Broker) replayWal() error {
	entries, err := b.zkClient.ReadWAL()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %v", err)
	}

	for _, entry := range entries {
		parts := strings.SplitN(string(entry), ":", 2)
		if len(parts) != 2 {
			log.Printf("Invalid WAL entry: %s", string(entry))
			continue
		}

		operation, data := parts[0], parts[1]
		switch operation {
		case "REGISTER_BROKER":
			err = b.zkClient.RegisterBroker(data)
			if err != nil {
				log.Printf("Failed to replay broker registration: %v", err)
			}
		case "CREATE_TOPIC":
			topicParts := strings.SplitN(data, ",", 2)
			if len(topicParts) != 2 {
				log.Printf("Invalid CREATE_TOPIC entry: %s", data)
				continue
			}
			partitionCount, err := strconv.Atoi(topicParts[1])
			if err != nil {
				log.Printf("Error in converting partitionCount to Integer")
			}
			err = b.CreateTopic(topicParts[0], partitionCount)
			if err != nil {
				log.Printf("Failed to replay topic creation: %v", err)
			}
		case "ASSIGN_PARTITION":
			partitionParts := strings.SplitN(data, ",", 3)
			if len(partitionParts) != 3 {
				log.Printf("Invalid ASSIGN_PARTITION entry: %s", data)
				continue
			}
			err = b.AssignPartition()
			if err != nil {
				log.Printf("Failed to replay partition assignment: %v", err)
			}
			// Other operations ???
		default:
			log.Printf("Unknown operation in WAL: %s", operation)
		}
	}
	return nil
}

func (b *Broker) loadTopicsAndPartitions() error {
	return nil
}

func (b *Broker) ProduceMessage(topic string, message []byte) error {
	b.producerLock.RLock()
	defer b.producerLock.RUnlock()

	partitions, exists := b.topics[topic]
}

func (b *Broker) ConsumeMessage(consumerID, topic string) ([]byte, error) {

}

func (b *Broker) heartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := b.zkClient.UpdateBrokerHeartBeat(b.ID)
			if err != nil {
				log.Printf("Failed to update heartbeat: %v", err)
			}
		case <-b.shutdownCh:
			return
		}
	}
}

func (b *Broker) monitorPartitions() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			b.checkPartitionLeaders()
		case <-b.shutdownCh:
			return

		}
	}
}

func (b *Broker) CreateTopic(name string, partitionCount int) error {
	err := b.zkClient.WriteAheadLog([]byte(fmt.Sprintf("CREATE_TOPIC:%s,%d", name, partitionCount)))
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}

	err = b.zkClient.RegisterTopic(name, partitionCount)
	if err != nil {
		return fmt.Errorf("failed to register topic: %v", err)
	}

	b.producerLock.Lock()
	defer b.producerLock.Unlock()

	b.topics[name] = make([]int, partitionCount)

	for i := 0; i < partitionCount; i++ {
		partitionID := len(b.partitions)
		b.partitions[partitionID] = &Partition{
			ID:    partitionID,
			Topic: name,
		}
		b.topics[name][i] = partitionID
	}

	return nil
}
