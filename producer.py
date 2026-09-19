import random, time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

TOPIC = "orders"
schema_str = open("schemas/order.avsc").read()
sr = SchemaRegistryClient({"url": "http://localhost:8081"})
serializer = AvroSerializer(sr, schema_str, lambda obj, ctx: obj)
producer = Producer({"bootstrap.servers": "localhost:9092"})

def on_delivery(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to {msg.topic()} [partition {msg.partition()}] offset {msg.offset()}")

order_id = 1001
try:
    while True:
        price = round(random.uniform(10, 500), 2)
        if random.random() < 0.1:          # ~10% bad orders, to show the DLQ working
            price = -price
        order = {"orderId": str(order_id), "product": f"Item{random.randint(1, 5)}", "price": price}
        producer.produce(TOPIC, key=order["orderId"],
                         value=serializer(order, SerializationContext(TOPIC, MessageField.VALUE)),
                         on_delivery=on_delivery)
        producer.poll(0)
        print("Produced:", order)
        order_id += 1
        time.sleep(1)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
