package producer

import (
	"fmt"
	"net"
	"os"
	"time"
)

type Producer struct {
	conn           net.Conn
	addr           string
	reconnectDelay time.Duration
}

func newProducer(addr string) *Producer {
	return &Producer{
		addr:           addr,
		reconnectDelay: time.Second,
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run producer.go <server_address:port>")
		os.Exit(1)
	}

	serverAddr := os.Args[1]
	producer := newProducer(serverAddr)

	err := producer.connect()
	if err != nil {
		fmt.Printf("Failed to connect to server: %v\n", err)
		os.Exit(1)
	}
	defer producer.Close()
}
