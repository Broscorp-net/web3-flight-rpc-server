# web3-flight-rpc-server
Blockchain data streaming in Apache Arrow format. Web3 data for data engineers, ready for large-scale analysis

## Project Overview

This is an implementation of [Apache Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html) server for blockchain data.
Apache Arrow format allows zero-copy streaming data processing on top of standard eth-web3 RPC.

There are a few key points why to consider flight rpc:
- Plenty of clients implementations including JavaScript, Go, Python, R and Rust (see: https://arrow.apache.org/docs/implementations.html)

- Rich libraries support. Process eth json-rpc data with your favorite data framework:
  - [pandas](https://pandas.pydata.org)
  - [polars](https://pola.rs/)
  - [apache datafusion](https://datafusion.apache.org/)
  - and many more natively supporting apache arrow format, all with zero copy and without time loss on serialization and deserialization between the formats

- [ADBC Support](https://arrow.apache.org/adbc/current/index.html) Ready-made connectors to most popular databases and formats allow you to write data effortlessly

## Supported Datasets

All datasets requests supports following params:

- **start_block block** number to start streaming from (default LATEST)
- **end_block** block number to stop streaming (default NULL)
- **batch_size**

### Blocks
See schema for stream return type
#### Schema
```java
private static final Schema BLOCK_SCHEMA = new Schema(List.of(
        Field.nullable(BLOCK_NUMBER, new ArrowType.Int(64, true)),
        Field.nullable(BLOCK_HASH_FIELD, new ArrowType.Utf8()),
        Field.nullable(BLOCK_PARENT_HASH, new ArrowType.Utf8()),
        Field.nullable(BLOCK_TIMESTAMP, new ArrowType.Int(64, true)),
        Field.nullable(BLOCK_AUTHOR, new ArrowType.Utf8()),
        Field.nullable(BLOCK_MINER, new ArrowType.Utf8()),
        Field.nullable(BLOCK_TRANSACTIONS_ROOT, new ArrowType.Utf8()),
        Field.nullable(BLOCK_STATE_ROOT, new ArrowType.Utf8()),
        Field.nullable(BLOCK_RECEIPTS_ROOT, new ArrowType.Utf8()),
        Field.nullable(BLOCK_LOGS_BLOOM, new ArrowType.Utf8()),
        Field.nullable(BLOCK_GAS_LIMIT, new ArrowType.Int(64, true)),
        Field.nullable(BLOCK_GAS_USED, new ArrowType.Int(64, true)),
        Field.nullable(BLOCK_SIZE, new ArrowType.Int(64, true)),
        Field.nullable(BLOCK_EXTRA_DATA, new ArrowType.Utf8()),
        Field.nullable(BLOCK_NONCE, new ArrowType.Utf8()),
        Field.nullable(BLOCK_DIFFICULTY, new ArrowType.Utf8()),
        Field.nullable(BLOCK_TOTAL_DIFFICULTY, new ArrowType.Utf8()),
        Field.nullable(BLOCK_MIX_HASH, new ArrowType.Utf8()),
        Field.nullable(BLOCK_SHA3_UNCLES, new ArrowType.Utf8()),
        new Field(BLOCK_TRANSACTIONS, new FieldType(true, new ArrowType.List(), null),
                Lists.newArrayList(Field.nullable(BLOCK_TX_CHILD_NAME, new ArrowType.Utf8()))),
        new Field(BLOCK_UNCLES, new FieldType(true, new ArrowType.List(), null),
                Lists.newArrayList(Field.nullable(BLOCK_UNCLE_CHILD_NAME, new ArrowType.Utf8()))),
        new Field(BLOCK_SEAL_FIELDS, new FieldType(true, new ArrowType.List(), null),
                Lists.newArrayList(Field.nullable(BLOCK_SEAL_FIELD_CHILD_NAME, new ArrowType.Utf8())))
));
```

### Logs
Additional parameters: 
- **topics** - list of topics to pass to web3 rpc filter
- **addresses** - list of addresses to pass to web3 rpc filter

#### Schema
```java
private static final Schema LOG_SCHEMA = new Schema(List.of(
        Field.nullable(LOG_ADDRESS, new ArrowType.Utf8()),
        Field.nullable(LOG_DATA, new ArrowType.Utf8()),
        new Field(LOG_TOPICS, new FieldType(true, new ArrowType.List(), null),
                Lists.newArrayList(Field.nullable(LOG_TOPIC_CHILD_NAME, new ArrowType.Utf8()))),
        Field.nullable(LOG_BLOCK_NUMBER, new ArrowType.Int(64, true)),
        Field.nullable(LOG_TRANSACTION_HASH, new ArrowType.Utf8()),
        Field.nullable(LOG_TRANSACTION_INDEX, new ArrowType.Int(32, true)),
        Field.nullable(LOG_BLOCK_HASH, new ArrowType.Utf8()),
        Field.nullable(LOG_INDEX, new ArrowType.Int(32, true)),
        Field.nullable(LOG_REMOVED, new ArrowType.Bool())
));
```

## Requirements

- Java 21
- Access to an Ethereum node (for the server). Infura was used for development.

## Quick Start with Docker

The easiest way to try the project is using Docker Compose with a local Hardhat node:

```bash
# Start Hardhat node and Flight server
docker-compose up -d

# Generate test data (optional - creates 50 transactions with logs)
docker-compose --profile test-data up test-data-generator

# View server logs
docker-compose logs -f flight-server

# Stop all services
docker-compose down
```

The Flight server will be available at `localhost:8815`, and Hardhat node at `localhost:8545`.

### Python Data Analysis with Jupyter

Launch Jupyter notebook for interactive data analysis:

```bash
# Start Jupyter along with other services
docker-compose --profile jupyter up -d

# View Jupyter logs to confirm startup
docker-compose logs jupyter

# Access Jupyter at: http://localhost:8888
# Token: ethereum
```

Open the notebook `ethereum_data_analysis.ipynb` to explore:
- Querying blockchain data via Arrow Flight
- Zero-copy conversion to Pandas
- Visualizing logs and blocks
- Analyzing gas usage, events, and transactions
- Filtering by contract address

**Note:** Generate test data first for meaningful analysis:
```bash
docker-compose --profile test-data up test-data-generator
```

## Building the Project

To build the project, run:

```bash
mvn clean package
```

This will create JAR file:
- `server/target/server.jar` - The server component

## Running the Applications

### Important: VM Options

Application requires the following VM options:

```
--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED
```

### Running the Server

The server requires an Ethereum node URL and can optionally specify a port for the Flight server.

**Using environment variables:**

```bash
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar server/target/server.jar
```

With environment variables:
- `ETHEREUM_NODE_URL` - URL of the Ethereum node (required)
- `FLIGHT_PORT` - Port for the Flight server (default: 8815)

**Using command line arguments:**

```bash
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar server/target/server.jar <ethereum_node_url> <flight_port>
```

Example:
```bash
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar server/target/server.jar https://mainnet.infura.io/v3/your-api-key 8815
```

### Running the Client

The client can connect to a Flight server at a specified host and port.

```bash
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar client/target/client.jar [host] [port]
```

By default, it connects to `127.0.0.1:8815` if no arguments are provided.

Example:
```bash
java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar client/target/client.jar 127.0.0.1 8815
```

## Example Workflow

1. Start the server with an Ethereum node URL:
   ```bash
   java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar server/target/server.jar https://mainnet.infura.io/v3/your-api-key 8815
   ```

2. In another terminal, start the client:
   ```bash
   java --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -jar client/target/client.jar 127.0.0.1 8815
   ```

3. The client will connect to the server and start receiving Ethereum logs data and displaying it in TSV format.

## Notes

- The server requires a valid Ethereum node URL to function properly
- For production use with Infura/Alchemy, set `ETHEREUM_NODE_URL` and `ETHEREUM_NODE_HTTP_URL` environment variables
- The Jupyter notebook provides a complete Python example for data analysis

## Python Client Example

See `notebooks/ethereum_data_analysis.ipynb` for a comprehensive example including:
- Connecting to Flight RPC server with `pyarrow`
- Querying logs and blocks
- Converting to Pandas DataFrames
- Data visualization with matplotlib/seaborn
- Filtering by contract address and topics

More examples (Polars, DataFusion, R) coming soon!