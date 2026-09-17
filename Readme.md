1. What does structured data mean?
Structured data means data that follows a well-defined structure/schema.


| sensor_id | temperature | humidity |
| --------: | ----------: | -------: |
|       101 |        29.5 |       72 |
|       102 |        30.1 |       68 |


This is structured because we know:

sensor_id    → integer
temperature  → float
humidity     → float


A simple analogy for Avro Serialization

Imagine sending this every time.:
Name: Venu
Age: 26
City: Batticaloa


Instead, both sides already agree:

1st value = Name
2nd value = Age
3rd value = City

So you can send:

Venu | 26 | Batticaloa

You don't need to repeatedly send Name:, Age:, and City:.
That's the basic idea behind Avro's compactness.