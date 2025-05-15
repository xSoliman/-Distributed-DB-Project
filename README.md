# Distributed Database System with Sharding and Replication

A simple distributed database system implemented in Go that combines sharding and replication. The system consists of a master node and multiple slave nodes, with automatic data distribution and synchronization.

## Features

* **Master-Slave Architecture**: Centralized master node with multiple slave nodes
* **Data Sharding**: Automatic data distribution across multiple shards
* **Synchronous Replication**: Real-time data synchronization between master and slaves
* **Concurrent Operations**: Efficient handling of concurrent database operations
* **Web Interface**: User-friendly web interface for database management
* **Custom Query Support**: Execute custom MySQL queries through the interface

## System Architecture

### Master Node

* Manages the primary database
* Handles database and table creation/deletion
* Distributes data across shards
* Synchronizes data with slave nodes
* Provides a web interface on port 8081

### Slave Nodes

* Maintains a copy of the data
* Receives updates from the master
* Handles read/write operations
* Synchronizes with master
* Provides a web interface on port 8082

### Sharding

* Data is automatically distributed across two shards
* Tables are assigned to shards based on a distribution algorithm
* Supports horizontal scaling

## Prerequisites

* Go 1.16 or higher
* MySQL Server
* Git

## Installation

1. Clone the repository:

```bash
git clone [repository-url]
cd [repository-name]
```

2. Install dependencies:

```bash
go mod download
```

3. Configure MySQL:

* Create a MySQL user with password '1234' || Change the password on the master.go & slave.go
* Ensure MySQL is running on localhost:3306

## Running the System

1. Start the Master node:

```bash
go run master.go
```

2. Start Slave nodes (replace \[master-ip] with your master's IP address):

```bash
go run slave.go [master-ip]
```

## Web Interface

* Master Interface: [http://localhost:8081](http://localhost:8081)
* Slave Interface: [http://localhost:8082](http://localhost:8082)

### Available Operations

* Create/Drop Database (Master only)
* Create/Drop Table (Master only)
* Select Data
* Insert Data
* Update Data
* Delete Data
* Custom MySQL Queries

## Project Structure

```
.
├── master.go           # Master node implementation
├── slave.go           # Slave node implementation
├── templates/         # HTML templates
│   └── index.html    # Main web interface template
├── static/           # Static web assets
│   ├── style.css    # CSS styles
│   └── script.js    # Frontend JavaScript
├── go.mod           # Go module file
└── go.sum           # Go module checksum
```

## Technical Details

### Replication

* Synchronous replication between the master and slaves
* Full database sync on slave initialization
* Real-time query propagation

### Sharding

* Two shard databases (shard1, shard2)
* Automatic table distribution
* Shard-aware query execution

### Concurrency

* Goroutine-based concurrent operations
* Mutex-protected critical sections
* TCP-based inter-node communication

## Acknowledgments

* Go programming language
* MySQL database
* Gin web framework
* jQuery for frontend functionality

## Team Members

* Soliman Adel Mohamed
* Saleh Abdelhadi
* Mahmoud Abdelbaset
* Hassan Ahmed
* Hassan Khaled
* Adham Mohamed
