# Real-Time Order Analytics and Failure Handling with Apache Kafka

An event-driven order processing system built on Apache Kafka. A producer service accepts purchase
orders over REST, serialises each one with Avro through the Confluent Schema Registry, and publishes
it to Kafka. A consumer service deserialises the order, processes it, and maintains a running average
of order prices in real time. Temporary processing failures are retried automatically with a delay;
permanent failures, and temporary failures that exhaust their retries, are routed to a Dead Letter
Queue so no message is silently lost.

## What the system does

| Capability | How it is implemented |
| --- | --- |
| **Avro serialisation** | Every Kafka message is a generated Avro `Order` record, registered in and validated against the Confluent Schema Registry. `order.avsc` is the single source of truth; the Java model is generated at build time. |
| **Real-time aggregation** | The consumer maintains `count`, `total` and `runningAverage`, updated only after an order is processed successfully, so failed and retried orders never distort the aggregate. |
| **Retry logic** | Temporary failures are retried across dedicated Kafka retry topics, up to three total processing attempts with a two-second delay between them. |
| **Dead Letter Queue** | Permanent validation errors bypass retry entirely and go straight to the DLQ. Temporary failures that exhaust all attempts follow them. The original Avro record is preserved, with the error context attached as Kafka headers. |

## The order message

Defined by the assignment brief in `order.avsc`:

| Field | Type | Description |
| --- | --- | --- |
| `orderId` | string | Unique identifier for the order (e.g. `"1001"`, `"1002"`) |
| `product` | string | Name of the purchased item (e.g. `"Item1"`, `"Item2"`) |
| `price` | float | Price of the product |

## Submission details

| | |
| --- | --- |
| **Student** | M.R.M.Ashfaq |
| **Registration number** | EG/2021/4417 |
| **Module** | EC 8202 — Big Data |
| **Assignment** | Assignment 3 (Chapter 3) |

## License

Released under the [MIT License](LICENSE).
