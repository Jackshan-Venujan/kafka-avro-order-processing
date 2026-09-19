import random, time
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

MAX_RETRIES = 3
schema_str = open("schemas/order.avsc").read()
sr = SchemaRegistryClient({"url": "http://localhost:8081"})
deserializer = AvroDeserializer(sr, schema_str, lambda d, ctx: d)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "order-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})
dlq_producer = Producer({"bootstrap.servers": "localhost:9092"})

class TemporaryError(Exception): pass
class PermanentError(Exception): pass

total, count = 0.0, 0

def process(order):
    if order["price"] <= 0:
        raise PermanentError(f"Invalid price {order['price']}")
    if random.random() < 0.2:
        raise TemporaryError("Simulated temporary failure (e.g. DB timeout)")

def send_to_dlq(msg, reason, attempts):
    dlq_producer.produce("orders-dlq", key=msg.key(), value=msg.value(),
                         headers={"error": reason, "attempts": str(attempts),
                                  "original_topic": msg.topic()})
    dlq_producer.flush()
    print(f"  -> Sent to DLQ: {reason}")

consumer.subscribe(["orders"])
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error()); continue

        try:
            order = deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        except Exception as e:
            send_to_dlq(msg, f"Deserialization failed: {e}", 0)
            consumer.commit(message=msg, asynchronous=False); continue

        print("Received:", order)
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                process(order)
                total += order["price"]; count += 1
                print(f"  OK | running average = {total / count:.2f} over {count} orders")
                break
            except TemporaryError as e:
                if attempt == MAX_RETRIES:
                    send_to_dlq(msg, f"Retries exhausted: {e}", attempt)
                else:
                    wait = 0.5 * 2 ** attempt
                    print(f"  Attempt {attempt} failed ({e}); retrying in {wait}s")
                    time.sleep(wait)
            except PermanentError as e:
                send_to_dlq(msg, str(e), attempt)
                break
        consumer.commit(message=msg, asynchronous=False)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
