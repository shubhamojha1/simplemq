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