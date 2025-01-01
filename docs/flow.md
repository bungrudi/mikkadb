# Redis-like Application Control Flow Documentation

## Main Components

### Core Components

- **Redis Core** (src/redis/core.rs)
  - Main Redis implementation
  - Manages storage and replication
  - Handles command execution and state management
  - Coordinates between all other components

- **Storage** (src/redis/storage.rs)
  - Thread-safe data storage using Mutex<HashMap>
  - Supports strings, lists, sets, and streams
  - Handles key expiration and value wrapping
  - Implements normalize_indices for list operations

- **ReplicationManager** (src/redis/replication.rs)
  - Manages master/replica relationships
  - Handles command replication queue
  - Tracks replication offset and replica status
  - Implements periodic GETACK mechanism

### Client Handling

- **ClientHandler** (src/client_handler.rs)
  - Manages individual client connections
  - Implements RESP protocol parsing
  - Handles transactions (MULTI/EXEC)
  - Routes commands to appropriate handlers

### Specialized Handlers

- **XReadHandler** (src/redis/xread_handler.rs)
  - Dedicated handler for Redis Streams XREAD
  - Supports blocking and non-blocking modes
  - Manages stream entry retrieval and filtering

### Persistence

- **RdbParser** (src/redis/rdb.rs)
  - Handles Redis database file parsing
  - Loads initial state from RDB files
  - Supports graceful handling of missing files

### Protocol

- **RESP Parser** (src/resp/command.rs)
  - Implements Redis protocol parsing
  - Converts raw input to RedisCommand enum
  - Handles all Redis protocol data types

## Startup Flow

```mermaid
graph TD
    A[Program Start] --> B[Parse Command Line Args]
    B --> C{--replicaof specified?}
    C -->|Yes| D[Start as Replica]
    C -->|No| E[Start as Master]
    E --> F[Load RDB File]
    E --> G[Start TCP Listener]
    E --> H[Start Replication Manager]
    D --> I[Connect to Master]
    D --> J[Start TCP Listener]
    D --> K[Begin Sync Process]
```

## Master Mode Operation

```mermaid
graph TD
    A[Master Node] --> B[Initialize Redis]
    B --> C[Load RDB]
    B --> D[Start TCP Listener]
    B --> E[Start Replication Manager]
    
    E --> F[Periodic Tasks]
    F --> G[Send Pending Commands]
    F --> H[Send GETACK]
    F --> I[Update Offsets]
    
    A --> J[Client Connections]
    J --> K[New ClientHandler]
    K --> L{Client Type?}
    L -->|Regular| M[Process Commands]
    L -->|Replica| N[Setup Replication]
```

### Command Processing (Master)

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ClientHandler
    participant R as Redis
    participant S as Storage
    participant RM as ReplicationManager
    participant REP as Replicas

    C->>CH: Send Command
    CH->>R: Process Command
    R->>S: Execute Command
    R->>RM: Enqueue for Replication
    RM->>REP: Propagate Command
    S-->>CH: Return Result
    CH-->>C: Send Response
```

## Replica Mode Operation

```mermaid
graph TD
    A[Replica Node] --> B[Initialize Redis]
    B --> C[Connect to Master]
    B --> D[Start TCP Listener]
    
    C --> E[Sync Process]
    E --> F[Track Bytes Processed]
    E --> G[Maintain Offset]
    E --> H[Process Master Commands]
    
    A --> I[Client Connections]
    I --> J[New ClientHandler]
    J --> K{Command Type?}
    K -->|Read| L[Process Locally]
    K -->|Write| M[Forward to Master]
```

### Command Processing (Replica)

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ClientHandler
    participant R as Replica
    participant M as Master
    participant S as Storage

    C->>CH: Send Command
    CH->>R: Process Command
    alt is Write Command
        R->>M: Forward Command
        M-->>R: Return Result
        R-->>CH: Forward Response
    else is Read Command
        R->>S: Execute Locally
        S-->>R: Return Result
    end
    CH-->>C: Send Response
```

## Key Components Interaction

```mermaid
graph TD
    subgraph Master
        A[ClientHandler] --> B[Redis Instance]
        B --> C[Storage]
        B --> D[ReplicationManager]
        D --> E[Replica Connections]
    end
    
    subgraph Replica
        F[ClientHandler] --> G[Redis Instance]
        G --> H[Storage]
        G --> I[Master Connection]
    end
    
    D -->|Replication Stream| G
```

## Transaction Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ClientHandler
    participant R as Redis
    participant S as Storage
    
    C->>CH: MULTI
    CH->>CH: Set in_transaction
    CH-->>C: OK
    
    loop Transaction Commands
        C->>CH: Command
        CH->>CH: Queue Command
        CH-->>C: QUEUED
    end
    
    C->>CH: EXEC
    activate CH
    loop Queued Commands
        CH->>R: Execute Command
        R->>S: Apply Changes
    end
    deactivate CH
    CH-->>C: Results Array
```

## XREAD Command Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant CH as ClientHandler
    participant XH as XReadHandler
    participant R as Redis
    participant S as Storage
    
    C->>CH: XREAD
    CH->>XH: Create Handler
    
    alt Blocking Mode
        loop Until Timeout/Data
            XH->>R: Check for New Data
            R->>S: Query Streams
            S-->>R: Return Entries
            R-->>XH: Return Results
            alt Has Data
                XH-->>CH: Return Results
                CH-->>C: Send Response
            else No Data
                XH->>XH: Sleep
            end
        end
    else Non-Blocking Mode
        XH->>R: Check for New Data
        R->>S: Query Streams
        S-->>R: Return Entries
        R-->>XH: Return Results
        XH-->>CH: Return Results
        CH-->>C: Send Response
    end
```

## Replication Sync Process

```mermaid
sequenceDiagram
    participant M as Master
    participant RM as ReplicationManager
    participant R as Replica
    participant S as Storage
    
    R->>M: Connect
    M->>RM: Register Replica
    
    loop Periodic Sync
        RM->>R: Send Pending Commands
        RM->>R: Send GETACK
        R-->>RM: ACK with Offset
        RM->>RM: Update Replica Status
    end
    
    alt Write Command Received
        M->>S: Execute Command
        M->>RM: Enqueue Command
        RM->>R: Propagate Command
        R->>S: Apply Change
        R-->>RM: Update Offset
    end
```
