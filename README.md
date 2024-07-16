## simplemq  

## Project Layout

```
simplemq/
│
├── cmd/
│   ├── producer/
│   │   └── main.go       # Entry point for the Producer application
│   ├── consumer/
│   │   └── main.go       # Entry point for the Consumer application
│   └── core_service/
│       └── main.go       # Entry point for the Core Service application
│
├── pkg/
│   ├── broker/
│   │   └── broker.go     # Broker logic including handling partitions
│   ├── datastore/
│   │   └── datastore.go  # Datastore logic for persisting messages
│   ├── zookeeper_client/
│   │   └── client.go     # Client to interact with Zookeeper for metadata, state, and coordination
│   └── utils/
│       └── utils.go      # Utility functions shared across components
│
├── api/
│   └── definitions/      # OpenAPI/Swagger definitions for API documentation
│
├── scripts/
│   └── setup_zookeeper.sh# Script to set up Zookeeper locally for testing
│
├── tests/
│   ├── integration/      # Integration tests for the system
│   └── unit/             # Unit tests for individual components
│
├── Dockerfile            # Dockerfile for containerization (optional)
├── docker-compose.yml     # Defines services, networks, and volumes for Docker (optional)
└── README.md              # Project documentation

```

## Commands
- Start Message Queue Server: go run ./cmd/core_service/main.go
- Send commands to the server: nc localhost 9092 | telnet localhost 9092
- Create Topic: CREATE_TOPIC|mytopic|3
- Produce a Message: PRODUCE|broker1|mytopic|0|Hello, World!
- Consume a Message: CONSUME|consumer1|mytopic|0
- Add a Broker: ADDBROKER|broker2
- Remove a Broker: REMOVEBROKER|broker2
- For Management API: 
    a. Get current broker count: curl http://localhost:8081/brokers
    b. Update broker count: curl -X POST -H "Content-Type: application/json" -d '{"broker_count": 3}' http://localhost:8081/brokers 
                                                                        OR 
                            Invoke-WebRequest -Uri http://localhost:8081/brokers -Method POST -Headers @{"Content-Type"="application/json"} -Body '{"broker_count": 3}'