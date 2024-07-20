package producer

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

type Producer struct {
	conn           net.Conn
	addr           string
	reconnectDelay time.Duration
}

func NewProducer(addr string) *Producer {
	return &Producer{
		addr:           addr,
		reconnectDelay: time.Second,
	}
}

func (p *Producer) connect() error {
	var err error
	p.conn, err = net.Dial("tcp", p.addr)
	return err
}

func (p *Producer) ensureConnected() error {
	if p.conn != nil {
		return nil
	}
	return p.connect()
}

func (p *Producer) reconnect() {
	for {
		err := p.connect()
		if err == nil {
			fmt.Println("Reconnected to server")
			return
		}
		fmt.Printf("Failed to reconnect: %v. Retrying in %v\n", err, p.reconnectDelay)
		time.Sleep(p.reconnectDelay)
	}
}

func (p *Producer) send(message string) (string, error) {
	err := p.ensureConnected()
	if err != nil {
		return "", err
	}

	_, err = fmt.Fprintf(p.conn, "%s\n", message)
	if err != nil {
		p.conn.Close()
		p.conn = nil
		go p.reconnect()
		return "", err
	}

	response, err := bufio.NewReader(p.conn).ReadString('\n')
	if err != nil {
		p.conn.Close()
		p.conn = nil
		go p.reconnect()
		return "", err
	}

	return strings.TrimSpace(response), nil
}

func (p *Producer) Close() {
	if p.conn != nil {
		p.conn.Close()
	}
}

func (p *Producer) ProduceBatch(broker, topic string, partition int, messages []string) error {
	batchMsg := fmt.Sprintf("PRODUCE_BATCH|%s|%s|%d|%d", broker, topic, partition, len(messages))
	for _, msg := range messages {
		batchMsg += "|" + msg
	}

	response, err := p.send(batchMsg)
	if err != nil {
		return err
	}

	fmt.Println("Server response: ", response)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run producer.go <server_address:port>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	producer := NewProducer(serverAddr)

	err := producer.connect()
	if err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()

	fmt.Println("Connected to server. Type 'quit' to exit.")
	fmt.Println("Format: PRODUCE|broker|topic|partition|message")
	fmt.Println("Or use BATCH to send multiple messages: BATCH|broker|topic|partition|message1|message2|...")

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		input := scanner.Text()
		if input == "quit" {
			break
		}

		parts := strings.Split(input, "|")
		if parts[0] == "BATCH" {
			if len(parts) < 5 {
				fmt.Println("Invalid format for batch. Use BATCH|broker|topic|partition|message1|message2|...")
				continue
			}

			broker, topic, partition := parts[1], parts[2], parts[3]
			messages := parts[4:]
			err := producer.ProduceBatch(broker, topic, atoi(partition), messages)
			if err != nil {
				fmt.Printf("Error sending batch: %v\n", err)
			}
		} else if len(parts) != 5 || parts[0] != "PRODUCE" {
			fmt.Println("Invalid format. Use: PRODUCE|broker|topic|partition|message")
			continue
		} else {
			response, err := producer.send(input)
			if err != nil {
				fmt.Printf("Error sending message: %v\n", err)
				continue
			}
			fmt.Println("Server response: ", response)
		}
	}
}

func atoi(s string) int {
	i, _ := strconv.Atoi(s)
	return i
}
