package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Consumer struct {
	conn           net.Conn
	addr           string
	consumerGroup  string
	topics         map[string]int // topic -> partition
	offsets        map[string]int // topic -> offset
	reconnectDelay time.Duration
	mu             sync.Mutex
}

func NewConsumer(addr, consumerGroup string) *Consumer {
	return &Consumer{
		addr:           addr,
		consumerGroup:  consumerGroup,
		topics:         make(map[string]int),
		offsets:        make(map[string]int),
		reconnectDelay: time.Second,
	}
}

func (c *Consumer) connect() error {
	var err error
	c.conn, err = net.Dial("tcp", c.addr)
	return err
}

func (c *Consumer) ensureConnected() error {
	if c.conn != nil {
		return nil
	}
	return c.connect()
}

func (c *Consumer) reconnect() {
	for {
		err := c.connect()
		if err == nil {
			fmt.Println("Reconnected to server")
			c.resubscribe()
			return
		}
		fmt.Printf("Failed to reconnect: %v. Retrying in %v\n", err, c.reconnectDelay)
		time.Sleep(c.reconnectDelay)
	}
}

func (c *Consumer) resubscribe() {
	for topic, partition := range c.topics {
		c.Subscribe(topic, partition)
	}
}

func (c *Consumer) send(message string) (string, error) {
	err := c.ensureConnected()
	if err != nil {
		return "", err
	}

	_, err = fmt.Fprintf(c.conn, "%s\n", message)
	if err != nil {
		c.conn.Close()
		c.conn = nil
		go c.reconnect()
		return "", err
	}

	response, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		c.conn.Close()
		c.conn = nil
		go c.reconnect()
		return "", err
	}

	return strings.TrimSpace(response), nil
}

func (c *Consumer) Subscribe(topic string, partition int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	cmd := fmt.Sprintf("SUBSCRIBE|%s|%s|%d", c.consumerGroup, topic, partition)
	response, err := c.send(cmd)
	if err != nil {
		return err
	}

	fmt.Println("Subscription response:", response)
	c.topics[topic] = partition
	c.offsets[topic] = 0 // Start consuming from the beginning
	return nil
}

func (c *Consumer) Consume(topic string) error {
	c.mu.Lock()
	partition, ok := c.topics[topic]
	offset := c.offsets[topic]
	c.mu.Unlock()

	if !ok {
		return fmt.Errorf("not subscribed to topic: %s", topic)
	}

	cmd := fmt.Sprintf("CONSUME|%s|%s|%d|%d", c.consumerGroup, topic, partition, offset)
	response, err := c.send(cmd)
	if err != nil {
		return err
	}

	fmt.Println("Received:", response)

	// Update offset
	c.mu.Lock()
	c.offsets[topic]++
	c.mu.Unlock()

	return nil
}

func (c *Consumer) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run consumer.go <server_address:port> <consumer_group>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	consumerGroup := os.Args[2]

	consumer := NewConsumer(serverAddr, consumerGroup)
	defer consumer.Close()

	fmt.Printf("Connected to server. Consumer Group: %s\n", consumerGroup)
	fmt.Println("Commands:")
	fmt.Println("SUBSCRIBE|topic|partition")
	fmt.Println("CONSUME|topic")
	fmt.Println("Type 'quit' to exit.")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		if input == "quit" {
			break
		}

		parts := strings.Split(input, "|")
		if len(parts) < 2 {
			fmt.Println("Invalid command format")
			continue
		}

		switch parts[0] {
		case "SUBSCRIBE":
			if len(parts) != 3 {
				fmt.Println("Invalid SUBSCRIBE format. Use: SUBSCRIBE|topic|partition")
				continue
			}
			topic, partition := parts[1], atoi(parts[2])
			err := consumer.Subscribe(topic, partition)
			if err != nil {
				fmt.Printf("Error subscribing: %v\n", err)
			}
		case "CONSUME":
			if len(parts) != 2 {
				fmt.Println("Invalid CONSUME format. Use: CONSUME|topic")
				continue
			}

			topic := parts[1]
			err := consumer.Consume(topic)
			if err != nil {
				fmt.Printf("Error consuming: %v\n", err)
			}
		default:
			fmt.Println("Unknown command")
		}
	}
}

func atoi(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}
