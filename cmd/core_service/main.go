package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

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

type BrokerConfig struct {
	BrokerCount int `json:"broker_count"`
}

const configFilePath = "./broker_config.json"

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
		return fmt.Errorf("broker %s already exists", id)
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

func init() {
	rand.Seed(time.Now().UnixNano())
}

func generateNewBrokerID() string {
	timestamp := time.Now().UnixNano()
	randomPart := rand.Intn(1000)
	return fmt.Sprintf("broker_%d_%d", timestamp, randomPart)
}

func (bm *BrokerManager) selectBrokerToRemove() string {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if len(bm.brokers) == 0 {
		return ""
	}

	brokerIDs := make([]string, 0, len(bm.brokers))
	for id := range bm.brokers {
		brokerIDs = append(brokerIDs, id)
	}

	sort.Strings(brokerIDs)

	return brokerIDs[len(brokerIDs)-1]
}

// func startManagementAPI(bm *BrokerManager) {
// 	http.HandleFunc("/brokers", func(w http.ResponseWriter, r *http.Request) {
// 		switch r.Method {
// 		case http.MethodGet:
// 			json.NewEncoder(w).Encode(BrokerConfig{BrokerCount: len(bm.brokers)})
// 		case http.MethodPost:
// 			var config BrokerConfig
// 			if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
// 				http.Error(w, err.Error(), http.StatusBadRequest)
// 				return
// 			}
// 			adjustBrokerCount(bm, config.BrokerCount)
// 			updateConfigFile(config)
// 			w.WriteHeader(http.StatusOK)
// 		default:
// 			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
// 		}
// 	})
// 	go http.ListenAndServe(":8080", nil)
// }

func startManagementAPI(bm *BrokerManager) {
	http.HandleFunc("/brokers", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Received request: %s %s", r.Method, r.URL.Path)
		switch r.Method {
		case http.MethodGet:
			json.NewEncoder(w).Encode(BrokerConfig{BrokerCount: len(bm.brokers)})
		case http.MethodPost:
			var config BrokerConfig
			if err := json.NewDecoder(r.Body).Decode(&config); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			adjustBrokerCount(bm, config.BrokerCount)
			updateConfigFile(config)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	log.Printf("Starting management API on :8081")
	go func() {
		if err := http.ListenAndServe(":8081", nil); err != nil {
			log.Fatalf("Failed to start management API: %v", err)
		}
	}()
}

func adjustBrokerCount(bm *BrokerManager, desiredCount int) {
	currentCount := len(bm.brokers)
	if desiredCount > currentCount {
		for i := 0; i < desiredCount-currentCount; i++ {
			bm.AddBroker(generateNewBrokerID())
		}
	} else if desiredCount < currentCount {
		for i := 0; i < currentCount-desiredCount; i++ {
			bm.RemoveBroker(bm.selectBrokerToRemove())
		}
	}
}

func updateConfigFile(config BrokerConfig) error {
	data, err := json.MarshalIndent(config, "", " ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(configFilePath, data, 0644)
}

func readConfigFile() (BrokerConfig, error) {
	var config BrokerConfig
	data, err := ioutil.ReadFile(configFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return BrokerConfig{BrokerCount: 1}, nil
		}
		return config, err
	}
	err = json.Unmarshal(data, &config)
	return config, err
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

	config, err := readConfigFile()
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	brokerManager := NewBrokerManager(zkClient, wal)

	adjustBrokerCount(brokerManager, config.BrokerCount)

	startManagementAPI(brokerManager)

	// Add initial broker
	// err = brokerManager.AddBroker("broker1")
	// if err != nil {
	// 	log.Fatalf("Failed to add initial broker: %v", err)
	// }

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

		if err := scanner.Err(); err != nil {
			log.Printf("Scanner error: %v", err)
			break
		}

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
			handleConsumeMessage(conn, bm, parts[1], parts[2], parts[3])
		case "CREATE_TOPIC":
			if len(parts) < 3 {
				log.Printf("Invalid CREATE_TOPIC command: %s", line)
				continue
			}
			handleCreateTopic(conn, bm, parts[1], parts[2])
		case "ADDBROKER":
			handleAddBroker(conn, bm, parts[1])
		case "REMOVEBROKER":
			handleRemoveBroker(conn, bm, parts[1])
		default:
			log.Printf("Unknown command: %s", parts[0])
		}
	}
}

func handleProduceMessage(conn net.Conn, bm *BrokerManager, brokerID, topic, partitionIDStr string, message []byte) {
	brk, exists := bm.brokers[brokerID]
	if !exists {
		fmt.Fprintf(conn, "ERROR: Broker %s does not exist\n", brokerID)
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
	}

	err = brk.ProduceMessage(topic, message)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Failed to produce message: %v\n", err)
	} else {
		fmt.Fprintf(conn, "OK: Message produced to topic %s, partition %d\n", topic, partitionID)
	}
}

func handleConsumeMessage(conn net.Conn, bm *BrokerManager, consumerID, topic, partitionIDStr string) {
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

func handleCreateTopic(conn net.Conn, bm *BrokerManager, topic, partitionsStr string) {
	partitions, err := strconv.Atoi(partitionsStr)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Invalid partition count\n")
		return
	}

	err = bm.zkClient.RegisterTopic(topic, partitions)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Failed to create topic: %v\n", err)
		return
	}

	// Create topic on all brokers
	// (in a real system, you'd distribute partitions across brokers).

	for _, brk := range bm.brokers {
		err := brk.CreateTopic(topic, partitions)
		if err != nil {
			fmt.Fprintf(conn, "ERROR: Failed to create topic on broker %s: %v\n", brk.ID, err)
			return
		}
	}
	fmt.Fprintf(conn, "OK: Topic %s created with %d partitions\n", topic, partitions)
}

func handleAddBroker(conn net.Conn, bm *BrokerManager, id string) {
	err := bm.AddBroker(id)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Failed to add broker: %v\n", err)
	} else {
		fmt.Fprintf(conn, "OK: Broker %s added\n", id)
	}
}

func handleRemoveBroker(conn net.Conn, bm *BrokerManager, id string) {
	err := bm.RemoveBroker(id)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Failed to remove broker: %v\n", err)
	} else {
		fmt.Fprintf(conn, "OK: Broker %s removed\n", id)
	}
}
