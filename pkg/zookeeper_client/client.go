package zookeeper_client

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	brokerPath = ""
	topicPath  = ""
)

type ZookeeperClient struct {
	conn            *zk.Conn
	walPath         string // write-ahead log
	brokersRootPath string
}

func NewZookeeperClient(address, walDir string) (*ZookeeperClient, error) {

	addresses := []string{address} // slice of strings

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel() Not necessary. need to look more.

	conn, _, err := zk.Connect(addresses, time.Second*5) // 5 secs timeout
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Zookeeper: %v", err)
	}

	/*
		Connect establishes a new connection to a pool of zookeeper servers.
		The provided session timeout sets the amount of time for which a session
		is considered valid after losing connection to a server. Within the session
		timeout it's possible to reestablish a connection to a different server and
		keep the same session. This is means any ephemeral nodes and watches are maintained.
	*/

	walFilePath := filepath.Join(walDir, "wal.log")
	err = os.MkdirAll(filepath.Dir(walFilePath), 0755)

	if err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	client := &ZookeeperClient{
		conn:            conn,
		walPath:         walFilePath,
		brokersRootPath: brokerPath,
	}

	// Ensure necessary Zookeeper paths exist
	for _, path := range []string{brokerPath, topicPath} {
		err = client.createPathIfNotExist(path)
		if err != nil {
			return nil, err
		}
	}

	return client, nil
}

func (z *ZookeeperClient) Close() {
	z.conn.Close()
}

func (z *ZookeeperClient) RegisterBroker(brokerName string) error {
	// .. create broker node in Zookeeper
	log.Printf("Registering broker %s in Zookeeper...", brokerName)
	// Actual Zookeeper operation to register the broker goes here

	// Construct the full path for the broker node
	brokerPath := filepath.Join(z.brokersRootPath, brokerName)

	log.Printf("Attempting to create broker at path: %s", brokerPath)

	// Attempt to create an ephemeral node for the broker
	_, err := z.conn.Create(brokerPath, []byte(brokerName), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	/*
		ACL - Access Control Lists - determine who can perform which operations.
		ACL is a combination of authentication scheme, an identity for that scheme, and a set of permissions
	*/
	if err != nil {
		if err == zk.ErrNodeExists {
			log.Printf("Broker %s is already registered.", brokerName)
			return nil // Node exists, which is expected for ephemeral nodes; return success
		}
		return fmt.Errorf("failed to register broker %s: %v", brokerName, err)
	}
	log.Printf("Broker %s successfully registered in Zookeeper.", brokerName)
	return nil
}

func (z *ZookeeperClient) RegisterTopic(topicName string, partitionCount int, brokerName string) error {
	// Implement registration of a topic and assignment to a broker in Zookeeper
	log.Printf("Registering topic %s with %d partitions to broker %s...", topicName, partitionCount, brokerName)
	// Actual Zookeeper operation to register the topic and assign it to a broker goes here
	return nil
}

// Additional methods for Zookeeper interactions
