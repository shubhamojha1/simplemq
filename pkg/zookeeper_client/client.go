package zookeeper_client

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

const (
	brokerPath = "/simplemq/brokers"
	topicPath  = "/simplemq/topics"
)

type ZookeeperClient struct {
	conn    *zk.Conn
	walPath string // write-ahead log
	// brokersRootPath string
}

func NewZookeeperClient(address, walDir string) (*ZookeeperClient, error) {

	// addresses := []string{address} // slice of strings

	// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	// defer cancel() Not necessary. need to look more.

	conn, _, err := zk.Connect([]string{"127.0.0.1:2181"}, time.Second*10) // 5 secs timeout
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

	// walFilePath := filepath.Join(walDir, "wal.log")
	walFilePath := fmt.Sprintf(walDir + "./wal.log")
	err = os.MkdirAll(filepath.Dir(walFilePath), 0755)

	if err != nil {
		return nil, fmt.Errorf("failed to create WAL directory: %v", err)
	}

	client := &ZookeeperClient{
		conn:    conn,
		walPath: walFilePath,
		// brokersRootPath: brokerPath,
	}

	// Ensure necessary Zookeeper paths exist
	for _, path := range []string{brokerPath, topicPath} {
		err = client.createPathIfNotExist(path)
		if err != nil {
			return nil, err
		}
	}

	// for _, path := range []string{"/my_znode"} {
	// 	err = client.createPathIfNotExist(path)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	return client, nil
}

func (z *ZookeeperClient) Close() error {
	if z.conn == nil {
		return fmt.Errorf("zookeeper connection is nil")
	}
	z.conn.Close()
	return nil
}

func (z *ZookeeperClient) createPathIfNotExist(path string) error {
	// exists, _, err := z.conn.Exists(path)
	// if err != nil {
	// 	return fmt.Errorf("failed to check if path exists: %v", err)
	// }
	// if !exists {
	data := []byte("my_data")
	// 	flags := int32(zk.FlagEphemeral)
	// 	// _, err = z.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
	// 	_, err = z.conn.Create(path, data, flags, zk.WorldACL(zk.PermAll))
	// 	if err != nil && err != zk.ErrNodeExists {
	// 		return fmt.Errorf("failed to create path: %v", err)
	// 	}
	// }
	// return nil

	// acl := zk.WorldACL(zk.PermAll)
	// _, err := z.conn.Create(path, data, 0, acl)
	// if err != nil {
	// 	log.Fatalf("Unable to create znode: %v", err)
	// }

	// ****************************************
	// works. but broker registration problem.
	parts := strings.Split(path, "/")
	for i := 1; i < len(parts); i++ {
		subPath := strings.Join(parts[:i+1], "/")
		exists, _, err := z.conn.Exists(subPath)
		if err != nil {
			return fmt.Errorf("unable to check if znode exists: %v", err)
		}

		if !exists {
			_, err := z.conn.Create(subPath, data, 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				return fmt.Errorf("unable to create znode: %v", err)
			}

		}

	}
	return nil
}

func (z *ZookeeperClient) RegisterBroker(brokerID string) error {
	log.Printf("Registering broker %s in Zookeeper...", brokerID)

	// Construct the full path for the broker node
	// brokerPath := filepath.Join(brokerPath, brokerID) # DO NOT USE filepath.
	brokerPath := fmt.Sprintf("/simplemq/brokers/%s", brokerID)

	log.Printf("Attempting to create broker at path: %s", brokerPath)

	// Attempt to create an ephemeral node for the broker
	// _, err := z.conn.Create(brokerPath, []byte(brokerID), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	_, err := z.conn.Create(brokerPath, []byte(brokerID), 0, zk.WorldACL(zk.PermAll))
	/*
		ACL - Access Control Lists - determine who can perform which operations.
		ACL is a combination of authentication scheme, an identity for that scheme, and a set of permissions
	*/
	if err != nil {
		if err == zk.ErrNodeExists {
			log.Printf("Broker %s is already registered.", brokerID)
			return nil // Node exists, which is expected for ephemeral nodes; return success
		}
		return fmt.Errorf("failed to register broker %s: %v", brokerID, err)
	}
	log.Printf("Broker %s successfully registered in Zookeeper.", brokerID)
	return nil
}

func (z *ZookeeperClient) DeleteBroker(brokerID string) error {
	brokerPath := fmt.Sprintf("/simplemq/brokers/%s", brokerID)
	err := z.conn.Delete(brokerPath, -1)
	if err != nil && err != zk.ErrNodeExists {
		return fmt.Errorf("failed to delete broker %s: %v", brokerID, err)
	}
	return nil
}

func (z *ZookeeperClient) DeleteHeartBeat(brokerID string) error {
	heartbeatPath := fmt.Sprintf("/simplemq/brokers/%s/heartbeat", brokerID)
	err := z.conn.Delete(heartbeatPath, -1)
	if err != nil && err != zk.ErrNodeExists {
		return fmt.Errorf("failed to delete heartbear for broker %s: %v", brokerID, err)
	}
	return nil
}

func (z *ZookeeperClient) RegisterTopic(topicName string, partitionCount int) error {
	// Implement registration of a topic and assignment to a broker in Zookeeper
	// log.Printf("Registering topic %s with %d partitions to broker %s...", topicName, partitionCount, brokerName)
	// Actual Zookeeper operation to register the topic and assign it to a broker goes here

	// topicPath := filepath.Join(topicPath, topicName)
	// topicPath := fmt.Sprintf(topicPath+"./%s", topicName)
	topicPath := fmt.Sprintf("%s/%s", topicPath, topicName)

	data := []byte(fmt.Sprintf("%d", partitionCount))
	_, err := z.conn.Create(topicPath, data, 0, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			log.Printf("Topic %s is already registered", topicName)
			return nil
		}
		return fmt.Errorf("failed to register topic %s: %v", topicName, err)
	}
	log.Printf("Topic %s successfully registered with %d partitions.", topicName, partitionCount)
	return nil
}

func (z *ZookeeperClient) WriteAheadLog(data []byte) error {
	file, err := os.OpenFile(z.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer file.Close()

	_, err = file.Write(append(data, '\r', '\n'))
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %v", err)
	}
	return nil
}

func (z *ZookeeperClient) ReadWAL() ([][]byte, error) {
	file, err := os.Open(z.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return [][]byte{}, nil
		}
		return nil, fmt.Errorf("failed to open WAL file: %v", err)
	}
	defer file.Close()

	var entries [][]byte
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		entries = append(entries, scanner.Bytes())
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading WAL: %v", err)
	}
	return entries, nil
}

func (z *ZookeeperClient) UpdateBrokerHeartBeat(brokerID string) error {
	// heartbeatPath := filepath.Join(brokerPath, brokerID, "heartbeat")

	heartbeatPath := fmt.Sprintf("%s/%s/heartbeat", brokerPath, brokerID)
	heartbeatValue := []byte(time.Now().String())

	// Check if heartbeat node exists
	exists, _, err := z.conn.Exists(heartbeatPath)
	if err != nil {
		return fmt.Errorf("failed to check existence of heartbeat node: %v", err)
	}
	if !exists {
		// Node doesnt exist. Create it
		_, err := z.conn.Create(heartbeatPath, heartbeatValue, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return fmt.Errorf("failed to create heartbeat node: %v", err)
		}
	}

	// Update the heartbeat in Zookeeper
	_, err = z.conn.Set(heartbeatPath, heartbeatValue, -1) // 0 OR another number -> requires that the node;s current version amtches the specifief version exactly.
	if err != nil {                                        // -1 -> ignores version check and updates the nodes regardless of its current version.
		return fmt.Errorf("failed to update broker heartbeat: %v", err)
	}

	fmt.Printf("Updated heartbeat for broker %s\n", brokerID)
	return nil

}
