# Redis Clone in Go

A lightweight Redis clone written in Go that implements core RESP2 (Redis Serialization Protocol) commands.

## Features
- **RESP Parsing:** Supports Redis protocol serialization and deserialization through custom RESP parsing algorithms 
- **Command Handling:** Implements basic and advanced Redis commands (`PING`, `ECHO`, `SET`, `GET`, `XREAD`, `BLPOP`, etc.) along with error handling
- **In-Memory Data Store:** Key-value storage using Go maps (string, list, and stream data types)
- **Publisher/Subscriber Messaging:** Implements a fan-out pattern using Go channels to disseminate messages from publishers to awaiting clients
- **Transaction Management:** Supports `MULTI` / `EXEC` commands to execute multiple operations atomically
- **Safe Concurrent Access:** Avoids race conditions and thread-unsafe operations (ex. concurrent insertions to data store) through the use of mutexes 
- **Connection Management:** Handles multiple TCP client connections concurrently using goroutines


## First-Time Setup

Prerequisites: Docker

### 1. Clone the repository
```bash
git clone https://github.com/aryand15/go-redis.git
cd go-redis
```

### 2. Build the Docker image
```bash
docker build -t go-redis .
```

## Running Locally

### 1. Start the server by running the Docker container
```bash
docker run --rm -p 6379:6379 go-redis
```

### 2. Connect one or multiple clients to the server (in separate terminals)
```bash
redis-cli -p 6379
```

### 3. Enter commands like:
```bash
PING
SET mykey "Hello"
GET mykey
```

## Future Improvements
- Implement persistence (AOF/RDB) so that data storage isn't completely ephemeral
- Implement replication (leader/follower) for improved fault tolerance & availability
- Benchmark performance


