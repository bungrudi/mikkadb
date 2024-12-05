# Rust Redis Implementation

A Redis compatible server implementation in Rust, developed as part of the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis) ([register here](https://app.codecrafters.io/r/energetic-camel-584480)). This implementation goes beyond basic Redis functionality to include advanced features like replication, streams, and RDB file support.

## Features

- **Core Redis Commands**: Supports fundamental operations like `PING`, `SET`, `GET`
- **Data Structures**: Implementation of strings, lists, sets, and streams
- **Replication**: Full master-replica support with:
  - Master/replica role configuration
  - Command replication queue
  - Offset tracking
  - Periodic GETACK mechanism
- **Streams**: Advanced stream operations including `XREAD` with:
  - Blocking and non-blocking modes
  - COUNT parameter support
  - Multiple stream handling
- **Persistence**: RDB file support for data persistence
- **Protocol**: Full RESP (Redis Serialization Protocol) implementation

## Architecture

The project is organized into several key components:

- **Core**: Central Redis implementation managing storage and replication
- **Storage**: Thread-safe data storage using Rust's concurrency primitives
- **Replication Manager**: Handles master/replica relationships and command propagation
- **Client Handler**: Manages connections and RESP protocol parsing
- **Specialized Handlers**: Dedicated handlers for complex operations like XREAD
- **RDB Parser**: Handles database file operations

### Threading Model

The server employs a multi-threaded architecture optimized for concurrent operations:

- **Main Thread**: Handles TCP connection acceptance and spawns client handler threads
- **Client Handler Threads**: One per connection, manages client communication and command parsing
- **Storage Thread**: Single thread for data storage operations, ensuring thread-safe access to shared state
- **Replication Thread**: Dedicated thread for managing master-replica synchronization
- **Background Thread**: Handles periodic tasks like expiry checking and GETACK mechanisms

This model ensures:
- Thread-safe access to shared resources through Rust's ownership system
- Efficient handling of concurrent client connections
- Clean separation of concerns between different server components
Note: this is a first pass implementation and understand that the threading model can be optimized for a i/o heavy app like this server. That's coming.

For detailed flow diagrams and component interactions, see `docs/flow.md`.

## Getting Started

1. Ensure you have `cargo (1.54 or later)` installed
2. Clone this repository
3. Run the server:
   ```sh
   ./spawn_redis_server.sh
   ```

### Running as a Replica

To start the server in replica mode:
```sh
./spawn_redis_server.sh --replicaof <master-host> <master-port>
```

## Development

1. The main implementation is in `src/main.rs` and the `src/redis/` directory
2. Make your changes
3. Commit and push:
   ```sh
   git add .
   git commit -m "your changes"
   git push origin master
   ```

## Testing

Run the test suite:
```sh
cargo test
```

For more information about the implementation details and specifications, refer to the documentation in the `docs/` directory.

## Why "Mikka"?

This project is dedicated to Mikka, my beloved cat who was more than just a pet - she was my faithful coding companion. For three years, especially during the challenging times of COVID, Mikka would sit beside me at my coding desk, serving as my "rubber duck" debugging partner (though she'd probably object to being called a duck!). Her presence was a constant source of comfort and inspiration as I worked through complex problems and late-night coding sessions.

Mikka passed away in November 2023, but her memory lives on in this project.  This is my tribute to a special friend who made the solitary hours of coding feel less lonely.