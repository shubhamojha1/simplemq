package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
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

}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.SplitN(line, "|", 3)
		if len(parts) < 3 {
			log.Printf("Invalid command: %s", line)
			return
		}

		switch parts[0] {
		case "SEND":
			handleSendMesage(conn, parts[1], parts[2])
		case "SUBSCRIBE":
			handleSubscribe(conn, parts[1])
		default:
			log.Printf("Unknown command: %s", parts[0])
		}
	}
}

func handleSendMessage(conn net.Conn, topic string, message string) {
	// Logic to send message to the broker, which then forwards it to the appropriate partition
	// Simplified: Directly print to connection for demonstration
	fmt.Fprintf(conn, "OK\n")
}

func handleSubscribe(conn net.Conn, topic string) {
	// Logic to subscribe to a topic and start sending messages to the consumer once available
	// Simplified: Keep connection open and continuously send messages
	for {
		fmt.Fprintf(conn, "message\n") // Example message sending loop
	}
}
