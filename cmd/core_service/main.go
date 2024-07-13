package main

import (
	"fmt"
	"log"

	"github.com/shubhamojha1/simplemq/pkg/broker"
	"github.com/shubhamojha1/simplemq/pkg/wal"
	"github.com/shubhamojha1/simplemq/pkg/zookeeper_client"
)

func main() {
	fmt.Println("Starting Core Service...")

	zkClient, err := zookeeper_client.NewZookeeperClient("127.0.0.1:2181", ".\\logs")
	if err != nil {
		log.Fatalf("Failed to connect to Zookeeper: %v", err)
	}
	defer zkClient.Close()

	// Initialize WAL
	wal, err := wal.NewWAL(".\\data")
	if err != nil {
		log.Fatalf("Failed to initialize WAL: %v", err)
	}

	// create and start broker
	brk := broker.NewBroker("broker1", zkClient, wal)
	err = brk.Start()
	if err != nil {
		log.Fatalf("Failed to start broker: %v", err)
	}

	// err = zkClient.RegisterBroker("broker1")
	// if err != nil {
	// 	log.Fatalf("Failed to register broker: %v", err)
	// }

	log.Println("Broker registration complete.")

}
