package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/shubhamojha1/simplemq/pkg/broker"
	"github.com/shubhamojha1/simplemq/pkg/wal"
	"github.com/shubhamojha1/simplemq/pkg/zookeeper_client"
)

type BrokerManager struct {
	brokers  map[string]*broker.Broker
	zkClient *zookeeper_client.ZookeeperClient
	wal      *wal.WAL
	mu       sync.Mutex
}

func NewBrokerManager(zkClient *zookeeper_client.ZookeeperClient, wal *wal.WAL) *BrokerManager {
	return &BrokerManager{
		brokers:  make(map[string]*broker.Broker),
		zkClient: zkClient,
		wal:      wal,
	}
}

func (bm *BrokerManager) AddBroker(id string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if _, exists := bm.brokers[id]; exists {
		return fmt.Errorf("broker %s already exists")
	}

	brk := broker.NewBroker(id, bm.zkClient, bm.wal)
	err := brk.Start()
	if err != nil {
		return fmt.Errorf("failed to start broker %s: %v", id, err)
	}

	bm.brokers[id] = brk
	return nil

}

func (bm *BrokerManager) RemoveBroker(id string) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	brk, exists := bm.brokers[id]
	if !exists {
		return fmt.Errorf("broker %s does not exist", id)
	}

	err := brk.Stop()
	if err != nil {
		return fmt.Errorf("failed to stop broker %s: %v", id, err)
	}

	delete(bm.brokers, id)
	return nil
}

func main() {
	fmt.Println("Starting Message Queue Server (Core Service)...")

	zkClient, err := zookeeper_client.NewZookeeperClient("127.0.0.1:2181", "./logs")
	if err != nil {
		log.Fatalf("Failed to connect to Zookeeper: %v", err)
	}
	defer zkClient.Close()

	// Initialize WAL
	wal, err := wal.NewWAL("./data")
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %v", err)
	}

	// // // create and start broker
	// brk := broker.NewBroker("broker1", zkClient, wal)
	// err = brk.Start()
	// if err != nil {
	// 	log.Fatalf("Failed to start broker: %v", err)
	// }

	// listener, err := net.Listen("tcp", ":9092")
	// if err != nil {
	// 	log.Printf("Failed to establish TCP listener: %v", err)
	// }
	// defer listener.Close()

	// for {
	// 	conn, err := listener.Accept()
	// 	if err != nil {
	// 		go handleConnection(conn)
	// 	} else {
	// 		log.Printf("Error acception connection: %v", err)
	// 	}
	// }

	// // log.Println("Broker registration complete.")
	// fmt.Println("Message Queue Server runnning...")
	// select {}

	brokerManager := NewBrokerManager(zkClient, wal)

	// Add initial broker
	err = brokerManager.AddBroker("broker1")
	if err != nil {
		log.Fatalf("Failed to add initial broker: %v", err)
	}

	listener, err := net.Listen("tcp", ":9092")
	if err != nil {
		log.Fatalf("Failed to establish TCP listener: %v", err)
	}
	defer listener.Close()

	fmt.Println("Message Queue Server Running...")

	for {
		conn, err := listener.Accept()
		if err == nil {
			go handleConnection(conn, brokerManager)
		} else {
			log.Printf("Error acception connection: %v", err)
		}
	}

}

func handleConnection(conn net.Conn, bm *BrokerManager) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "|", 5)
		if len(parts) < 2 {
			log.Printf("Invalid command: %s", line)
			continue
		}

		switch parts[0] {
		case "PRODUCE":
			if len(parts) < 5 {
				log.Printf("Invalid PRODUCE command: %s", line)
				continue
			}
			handleProduceMessage(conn, bm, parts[1], parts[2], parts[3], []byte(parts[4]))
		case "CONSUME":
			if len(parts) < 4 {
				log.Printf("Invalid CONSUME command: %s", line)
				continue
			}
			handleConsumeMessage()
		case "CREATE_TOPIC":
			if len(parts) < 3 {
				log.Printf("Invalid CREATE_TOPIC command: %s", line)
				continue
			}
			handleCreateTopic()
		case "ADDBROKER":
			handleAddBroker()
		case "REMOVEBROKER":
			handleRemoveBroker()
		default:
			log.Printf("Unknown command: %s", parts[0])
		}
	}
}

func handleProduceMessage(conn net.Conn, bm *BrokerManager, brokerID, topic, partitionIDStr string, message []byte) {
	brk, exists := bm.brokers[brokerID]
	if !exists {
		fmt.Println(conn, "ERROR: Broker %s (%s) does not exist\n", brokerID, brk)
		return
	}

	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Invalid partition ID: %v\n", err)
		return
	}

	err = bm.wal.Append(topic, partitionID, message)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Failed to write to WAL: %v\n", err)
		return
	} else {
		fmt.Fprintf(conn, "OK: Message produced to topic %s, partition %d\n", topic, partitionID)
	}
}

func handleConsumeMessage(conn net.Conn, bm *BrokerManager, brokerID, topic, partitionIDStr string, message []byte) {
	partitionID, err := strconv.Atoi(partitionIDStr)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Invalid partition ID: %v\n", err)
		return
	}

	messages, err := bm.wal.Read(topic, partitionID)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Failed to read from WAL: %v\n", err)
		return
	}

	if len(messages) == 0 {
		fmt.Fprintf(conn, "No messages available for topic %s, partition %d\n", topic, partitionID)
		return
	}

	for _, message := range messages {
		fmt.Fprintf(conn, "Message: %s\n", string(message))
	}
}
