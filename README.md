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

## Running Server Locally
Make sure you have redis-cli installed on your machine first if you want to spin up a client.

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

Make sure to rebuild the container any time a change is made to the source code of the application.


## Benchmarking
It is important to collect performance data so that we can see how well the server handles multiple concurrent clients, each of which may make many requests in a short period of time. To accomplish this, I used Redis' official `redis-benchmark` command-line tool.

### Benchmarking Process
1) Run my Redis implementation (go-redis), starting the server on port 6379.
2) Have 50 clients complete 100000 GET requests and 100000 SET requests in total (number of clients, total requests, and commands can be customized in benchmark script).
3) Record throughput (requests per second) for both GET and SET.
4) Record P99 latency (milliseconds) for both GET and SET. 99% of requests finish faster than the P99 latency.
5) Stop the go-redis server, and repeat the same process for the official Redis server.
6) Compare how well my implementation did compared to the official Redis implementation.

### Running Benchmark Locally

#### 1. Build the Docker image, targeting the `benchmark` stage

This contains the necessary `redis-tools` library containing `redis-benchmark` and `redis-server` (for the official Redis server), as well as the compiled binary containing my implementation, all packaged into a single Debian Linux image for convenience. Rebuild the image any time the code is changed for the most up-to-date benchmarking results.
```bash
docker build --target benchmark -t go-redis-benchmark
```

#### 2. Run the container from the image
This will run the `benchmark/benchmark.sh` script in the container.
```bash
docker run --rm go-redis-benchmark
```

### Results
After running the benchmark locally through the Docker container (with 50 concurrent clients, 100000 total requests each for SET and GET):


| Metric | go-redis | Official Redis |
| ------ | -------- | -------------- |
| **GET RPS** | 233,644.86 | 261,780.11 |
| **GET P99 (msec)** | 0.223 | 0.167 |
| **SET RPS** | 226,244.34 | 255,102.05 |
| **SET P99 (msec)** | 0.263 | 0.159 |

These results show that my implementation achieves **~89% of official Redis throughput** for both GET and SET operations. P99 latencies remain **under 0.3ms**, and it handles **200K+ requests/second** with 50 concurrent clients.

<em>Note that these absolute performance numbers will vary based on hardware, which is why it's more meaningful to perform a relative comparison between my implementation and the official Redis implementation on the same hardware.</em>


## Future Improvements
- Implement persistence (AOF/RDB) so that data storage isn't completely ephemeral
- Implement replication (leader/follower) for improved fault tolerance & availability


