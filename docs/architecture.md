# Architecture and Design Decisions

This document records *why* the system is built the way it is. The README describes what it does
and how to run it; this is the reasoning behind the choices, including the ones that were made
after something went wrong during development.

## The pipeline

```text
Swagger UI ──JSON──▶ producer-service ──Avro Order──▶ orders.v1 ──▶ consumer-service
                          │                                              │
                          └──registers schema──▶ Schema Registry ◀──resolves schema──┘
                                                                         │
                       success ─────────────────────────────────────────▶ running average
                       temporary failure ──▶ orders.v1-retry-0 ──▶ (2 s) ──▶ retry
                                        ──▶ orders.v1-retry-1 ──▶ (2 s) ──▶ retry
                       permanent failure, or retries exhausted ──────────▶ orders.v1-dlt
```

Three modules, one schema:

| Module | Role |
| --- | --- |
| `common-avro` | Holds `order.avsc` and generates `Order.java` from it at build time. The schema is the single source of truth; the generated class is never committed. |
| `producer-service` | Turns REST requests into Avro records and publishes them with `orderId` as the message key. |
| `consumer-service` | Consumes, validates, aggregates, retries and dead-letters. Also exposes the aggregate over HTTP. |

## One partition, one consumer

`orders.v1` has a single partition and the consumer runs one listener thread. This is deliberate.

The assignment asks for *the* running average — a single global number. With several partitions
and several consumer threads, each would hold a partial aggregate, and combining them correctly is a
stream-processing problem in its own right (Kafka Streams with a state store, or a repartition by a
constant key). For a demonstration whose value lies in a reproducible, predictable average, one
partition removes that problem entirely: every order is processed in the order it was published, and
the aggregate after each step is exactly what the acceptance table says it should be.

The trade-off is throughput, which is irrelevant here.

## The aggregate updates only after success

The single most important line in the consumer is the order of operations in
`OrderListener.onOrder`: process first, record second. An order that throws never reaches the
counter. This is what keeps failed and retried orders out of the average — the specific mistake the
assignment calls out.

Two further details make this robust rather than merely correct on the happy path:

- **Idempotency lives inside the aggregate.** `RunningAverage` keeps the set of counted order
  identifiers and checks it in the same `synchronized` block as the counter update. An order that
  fails twice and succeeds on its third attempt contributes exactly once, and the unit tests prove
  this holds under 32 threads recording the same order simultaneously.
- **Reads are consistent.** `snapshot()` returns `count`, `total` and `average` as one immutable
  record taken under the same lock. `GET /api/stats` reads from an HTTP thread while the listener
  thread writes; fetching the fields separately could return a triple that never actually existed.

The total is a `double`, not a `float`. Prices arrive as floats, but accumulating many of them in
float precision drifts, and the average is the headline number.

## `price` is a `float`

The brief defines the schema with `price` as `float`, so that is what `order.avsc` declares. A
`decimal` logical type would be the right choice for real money — binary floating point cannot
represent 0.10 exactly — but the schema is fixed by the assignment, and matching it exactly matters
more than improving on it. The consumer formats money to two decimals for display, which is a
presentation choice, not a change to the stored value.

## Retry topics, and the naming trap

Retries are non-blocking: rather than sleeping on the consumer thread, the failed record is
republished to a retry topic and picked up again after the backoff. Spring Kafka's `@RetryableTopic`
does this, but two of its defaults produce the wrong topic names for this project.

With a fixed backoff, the defaults are `SUFFIX_WITH_DELAY_VALUE` and `SINGLE_TOPIC`: one retry topic
named by the delay, `orders.v1-retry-2000`. The project pre-creates `orders.v1-retry-0` and
`orders.v1-retry-1`, and broker-side topic auto-creation is disabled — a typo in a topic name must
fail loudly rather than silently invent an empty topic. So the listener sets **both**
`sameIntervalTopicReuseStrategy = MULTIPLE_TOPICS` and
`topicSuffixingStrategy = SUFFIX_WITH_INDEX_VALUE`, which makes the generated names match the
pre-created ones exactly. These two attributes were verified against the resolved Spring Kafka jar,
not assumed.

Without them the retry path breaks *silently*: the happy path still works, and only a temporary
failure reveals that its record has nowhere to go.

## The consumer is also a producer

Retry and dead-letter routing work by republishing, so the consumer needs a `KafkaTemplate` that can
serialise an Avro `Order`. Spring Boot's auto-configured template uses `StringSerializer`; the first
version of the consumer failed with `Can't convert value of class ...avro.Order` and redelivered the
record from the original topic forever.

The dead-letter publisher also has to handle a **poison pill** — bytes on the topic that were never
valid Avro. The error-handling deserialiser cannot turn these into an `Order`, so what gets forwarded
is the raw byte array, and an Avro serialiser rejects that (`Error registering Avro schema "bytes"`).
Because that failure happens inside the publisher, the record is never recovered, its offset is never
committed, and one malformed message stops the consumer dead.

`KafkaPublishingConfig` therefore uses a `DelegatingByTypeSerializer`: `Order` goes through the Avro
serialiser, `byte[]` through `ByteArraySerializer`. Each kind of value gets the serialiser it needs,
and a poison pill lands on the dead letter queue like any other failure.

The retry and dead-letter topics need their own Schema Registry subjects (`orders.v1-retry-0-value`
and so on), so this producer keeps `auto.register.schemas` enabled. The producer service only ever
registers `orders.v1-value`.

## Attempt counting comes from Spring, not from memory

The `TEMP_FAIL` rule needs to know which attempt it is on. Rather than keeping an in-memory counter
per order identifier, the listener reads the `retry_topic-attempts` header that Spring stamps on
every retry record. This is stateless: it survives a consumer restart, and re-running the
demonstration with the same order identifier behaves identically instead of succeeding immediately
because the JVM remembers three prior attempts. The header is absent on the original topic, which
the code treats as attempt 1.

## What the DLQ record contains

The dead-lettered record is the **original Avro order**, unchanged. Spring attaches the error
context as headers: the exception class and message, and the original topic, partition and offset.
The record therefore stays inspectable in Kafka UI and could be replayed to `orders.v1` once the
underlying cause is fixed.

One detail worth recording because it is easy to get wrong: the headers `@RetryableTopic` writes
are named `kafka_original-*` and `kafka_exception-*`, not the `kafka_dlt-exception-*` constants one
would reach for first. The `[DLQ]` log line reads them through a small helper that decodes the
numeric headers from Kafka's big-endian byte layout.

## No Schema Registry Maven plugin

The Confluent Maven plugin can register or compatibility-check a schema at build time. It was
deliberately left out: it needs a running Schema Registry, which would break the requirement that
`./mvnw clean verify` passes with no Docker. The producer registers the schema on first publish, and
the integration tests use the in-memory `mock://` registry.

## Retry timing

The two-second backoff is a **minimum**, not an exact interval. In a clean run the observed gaps
between attempts ranged from 2.0 s to 5.9 s; the longer ones were the first hop onto a retry topic
whose listener had been idle, where the poll cycle adds latency before the backoff even starts. The
timestamps in the consumer log make this visible, and the documentation states it rather than
claiming exact two-second gaps.

## Known limitations

- **The running average is in memory.** A consumer restart resets it to zero, and because offsets
  are committed it does not rebuild from topic history. A production system would persist the
  aggregate or recompute it from a compacted topic on startup.
- **The set of counted order identifiers grows without bound.** Fine for a demonstration
  processing a handful of orders; a production system would externalise it with a retention window.
- **One partition caps throughput.** Chosen for determinism, as explained above.
- **The dead-letter handler cannot deserialise a poison pill either.** Spring logs that no further
  action will be taken and commits the offset, so the consumer is not blocked; the record stays on
  the DLQ for inspection with its `DeserializationException` cause in the headers.
