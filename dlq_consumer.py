from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

schema_str = open("schemas/order.avsc").read()
sr = SchemaRegistryClient({"url": "http://localhost:8081"})
deserializer = AvroDeserializer(sr, schema_str, lambda d, ctx: d)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "dlq-inspector",          # different group -> independent offsets
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["orders-dlq"])

print("Listening for failed orders on orders-dlq...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error:", msg.error())
            continue

        # Headers come back as a list of (key, bytes) tuples, or None
        headers = {k: v.decode() for k, v in (msg.headers() or [])}

        try:
            order = deserializer(msg.value(), SerializationContext("orders", MessageField.VALUE))
        except Exception as e:
            order = f"<could not deserialize: {e}>"

        print("DLQ message:")
        print("  order   :", order)
        print("  error   :", headers.get("error"))
        print("  attempts:", headers.get("attempts"))
        print("  from    :", headers.get("original_topic"))
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
