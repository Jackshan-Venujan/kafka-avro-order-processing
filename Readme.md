# Kafka Avro Order Processing

EC8203 Mini Project: a Kafka-based system that produces and consumes **order messages** serialized with **Avro**, with:

- **Real-time aggregation**: a running average of order prices
- **Retry logic** for temporary failures, with exponential backoff
- **Dead Letter Queue (DLQ)** for messages that fail permanently

Everything is written in **Python** (`confluent-kafka`). Kafka, Schema Registry and Kafka UI run in **Docker**.

---

## Architecture

```
                       ┌──────────────────────────────┐
                       │   Schema Registry  :8081     │
                       │   stores order.avsc          │
                       └──────▲───────────────▲───────┘
                  register /  │               │  fetch schema
                  look up ID  │               │  by ID
┌──────────────┐   Avro     ┌─┴───────────────┴──┐
│ producer.py  │──────────► │   topic: orders    │
│ random orders│  key =     │   (3 partitions)   │
└──────────────┘  orderId   └─────────┬──────────┘
                                      │
                                      ▼
                        ┌──────────────────────────────┐
                        │ consumer.py                  │
                        │ group: order-consumer-group  │
                        │                              │
                        │ success ─► running average   │
                        │ temporary error ─► retry     │
                        │   (1s, 2s backoff, 3 tries)  │
                        │ permanent error / retries    │
                        │   exhausted ─► DLQ           │
                        └──────────────┬───────────────┘
                                       │ original bytes + error headers
                                       ▼
┌──────────────────┐    ┌──────────────────────────────┐
│ dlq_consumer.py  │◄───│   topic: orders-dlq          │
│ group:           │    │   (1 partition)              │
│ dlq-inspector    │    └──────────────────────────────┘
└──────────────────┘

Kafka UI (:8080) is used to inspect topics, messages, consumer groups and schemas.
```

### Components

| Component | Role |
|---|---|
| `kafka` (Docker) | Single Kafka broker in KRaft mode (no ZooKeeper). Host clients connect on `localhost:9092`; containers use `kafka:29092`. |
| `schema-registry` (Docker) | Stores the Avro schema. Messages carry a 5-byte header (magic byte + schema ID) instead of the full schema. |
| `kafka-ui` (Docker) | Web UI at http://localhost:8080 |
| `producer.py` | Sends one random order per second to `orders`. About 10% have a negative price, to exercise the DLQ. |
| `consumer.py` | Deserializes orders, processes them with retries, keeps the running average and routes failures to `orders-dlq`. |
| `dlq_consumer.py` | Reads `orders-dlq` and prints each failed order with its error details. |

### Order schema (`schemas/order.avsc`)

| Field | Type | Description |
|---|---|---|
| `orderId` | string | Unique order ID (`"1001"`, `"1002"`, …) |
| `product` | string | Product name (`"Item1"` … `"Item5"`) |
| `price` | float | Randomized price between 10 and 500 |

---

## Project structure

```
.
├── docker-compose.yml     # Kafka, Schema Registry, Kafka UI
├── schemas/
│   └── order.avsc         # Avro schema for order messages
├── producer.py            # Order producer
├── consumer.py            # Aggregation + retry + DLQ consumer
├── dlq_consumer.py        # DLQ inspector
├── requirements.txt       # Python dependencies
└── Readme.md
```

---

## Prerequisites

- **Docker Desktop**, running
- **Python 3.10+**
- Free ports: `9092`, `8081`, `8080`

---

## How to run

### 1. Start the infrastructure

```powershell
docker compose up -d
docker compose ps        # all three containers should be "running"
```

### 2. Create the topics

```powershell
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic orders-dlq --partitions 1 --replication-factor 1
```

> Create the topics **before** running the scripts. Otherwise Kafka auto-creates `orders` with a single partition.

### 3. Install the Python dependencies

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### 4. Run the scripts

Open three terminals in the project folder and activate the venv in each (`venv\Scripts\activate`). Start the consumers first:

```powershell
# Terminal 1
python consumer.py

# Terminal 2
python dlq_consumer.py

# Terminal 3
python producer.py
```

Stop any script with `Ctrl+C`.

### 5. Inspect in Kafka UI

Open http://localhost:8080:

- **Topics → orders → Messages**: set *Value Serde* to `SchemaRegistry` to see the decoded Avro messages
- **Topics → orders-dlq → Messages**: expand a message to see its error headers
- **Consumers**: lag per partition for `order-consumer-group` and `dlq-inspector`
- **Schema Registry**: the `orders-value` subject (version, ID, compatibility)

### 6. Stop

```powershell
docker compose down       # stop containers, keep data
docker compose down -v    # stop and delete all Kafka data (clean reset)
```

---

## How retries and the DLQ work

Each message from `orders` goes through this flow in `consumer.py`:

```
deserialize ──fails──────────────────────────────► DLQ (attempts = 0)
     │
     ▼
process(order)  ── attempt 1..3
     ├─ success           ─► update running average ─► commit offset
     ├─ TemporaryError    ─► wait 0.5 × 2^attempt seconds, retry
     │                       (after attempt 3)     ─► DLQ
     └─ PermanentError    ─► DLQ immediately (no retry)
```

### Temporary vs permanent errors

| Type | Example (simulated) | Handling |
|---|---|---|
| **Temporary** | A random 20% "DB timeout". The same message may succeed on the next try. | Retried with exponential backoff |
| **Permanent** | `price <= 0`. The data itself is invalid, so retrying can never succeed. | Sent straight to the DLQ |
| **Poison message** | Bytes that are not valid Avro | Sent straight to the DLQ |

### Backoff

| Attempt | Result if it fails |
|---|---|
| 1 | wait **1 s**, retry |
| 2 | wait **2 s**, retry |
| 3 | give up, send to **DLQ** |

### DLQ message format

The DLQ message keeps the **original key and original Avro bytes** unchanged, so it can be inspected or replayed later. The failure details go in Kafka **headers**:

| Header | Example |
|---|---|
| `error` | `Invalid price -87.30000305175781` or `Retries exhausted: Simulated temporary failure (e.g. DB timeout)` |
| `attempts` | `1` |
| `original_topic` | `orders` |

### Delivery guarantee

Auto-commit is disabled (`enable.auto.commit=False`). The offset is committed only **after** a message has been fully handled, either processed successfully or written to the DLQ. If the consumer crashes mid-processing, the message is read again on restart (**at-least-once** delivery), so no order is lost.

---

## Sample output

**producer.py**
```
Produced: {'orderId': '1001', 'product': 'Item3', 'price': 245.67}
Produced: {'orderId': '1002', 'product': 'Item1', 'price': 132.4}
Sent to orders [partition 1] offset 0
Produced: {'orderId': '1003', 'product': 'Item5', 'price': 410.05}
Sent to orders [partition 0] offset 0
Produced: {'orderId': '1004', 'product': 'Item2', 'price': -87.3}
Sent to orders [partition 2] offset 0
```

**consumer.py**
```
Received: {'orderId': '1001', 'product': 'Item3', 'price': 245.6699981689453}
  OK | running average = 245.67 over 1 orders
Received: {'orderId': '1002', 'product': 'Item1', 'price': 132.39999389648438}
  Attempt 1 failed (Simulated temporary failure (e.g. DB timeout)); retrying in 1.0s
  OK | running average = 189.03 over 2 orders
Received: {'orderId': '1003', 'product': 'Item5', 'price': 410.04998779296875}
  OK | running average = 262.71 over 3 orders
Received: {'orderId': '1004', 'product': 'Item2', 'price': -87.30000305175781}
  -> Sent to DLQ: Invalid price -87.30000305175781
Received: {'orderId': '1005', 'product': 'Item4', 'price': 58.900001525878906}
  OK | running average = 211.75 over 4 orders
```

**dlq_consumer.py**
```
Listening for failed orders on orders-dlq...
DLQ message:
  order   : {'orderId': '1004', 'product': 'Item2', 'price': -87.30000305175781}
  error   : Invalid price -87.30000305175781
  attempts: 1
  from    : orders
```

> Prices such as `245.6699981689453` appear because the schema uses Avro `float` (32-bit), as the assignment specifies. The running average is rounded to 2 decimals.

<!-- Add Kafka UI screenshots here, e.g.
![Messages in orders topic](docs/kafka-ui-orders.png)
![DLQ message headers](docs/kafka-ui-dlq.png)
-->

---

## Why Avro

With JSON, every message repeats its field names:

```json
{"orderId": "1001", "product": "Item3", "price": 245.67}
```

With Avro, the producer and consumer **agree on the schema in advance**, so only the values are sent in compact binary: `1001 | Item3 | 245.67`. The field names are never repeated.

Benefits in this project:

- **Smaller messages**: no field names, binary encoding
- **Enforced structure**: an order with a missing field or wrong type cannot be serialized
- **Schema evolution**: Schema Registry versions the schema and rejects incompatible changes (default: `BACKWARD` compatibility)
- **Readable tooling**: Kafka UI decodes messages by looking up the schema ID in Schema Registry
