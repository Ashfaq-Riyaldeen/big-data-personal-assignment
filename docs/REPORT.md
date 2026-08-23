# Design Report

**EC 8202 Big Data — Chapter 3 Assignment**
Kafka producer/consumer for Avro order messages, with real-time aggregation, retry logic and a
dead letter queue.

---

## 1. Scope

The brief asks for a Kafka system that produces and consumes order messages, where every message
uses Avro serialisation, and the system supports real-time aggregation of a running average
price, retry logic for temporary failures, and a dead letter queue for permanently failed
messages.

Those four requirements are easy to satisfy superficially and interesting to satisfy properly.
This report covers the decisions where a naive implementation and a correct one diverge, and it
records the alternatives that were considered and rejected.

The implementation is Java 17 against Kafka 3.9 running in KRaft mode, with the Confluent Schema
Registry, all defined in Docker Compose.

---

## 2. Serialisation: Avro and the Schema Registry

### 2.1 Why Avro rather than JSON

JSON is self-describing, which sounds like a virtue until it is priced. Every message repeats
every field name, so `{"orderId":"1001","product":"Item1","price":42.5}` spends more bytes on
describing itself than on data. Avro writes the values only, in schema order — around 20 bytes
for the same record — because the reader already knows the shape.

More importantly, JSON has no notion of a contract. A producer that starts sending `price` as a
string breaks its consumers at runtime, in production, one message at a time. Avro's schema is
declared up front and enforced at serialisation.

### 2.2 The Schema Registry and the wire format

The schema is not sent with each message. Instead the producer registers `order.avsc` with the
Schema Registry, which assigns it an id, and each message carries only that id:

```
 byte 0      bytes 1-4              bytes 5..n
+--------+------------------+-------------------------+
|  0x00  |   schema id (4)  |   Avro-encoded payload  |
+--------+------------------+-------------------------+
  magic       big-endian            no field names
```

Five bytes of overhead buys a guarantee: any consumer, at any time, can fetch the exact schema a
message was written with. That matters for **schema evolution**. The registry is configured with
`BACKWARD` compatibility, so a proposed new version is rejected outright if existing consumers
could not read it — the incompatibility is caught at deploy time rather than discovered from a
consumer crash loop at three in the morning.

### 2.3 Code generation

`Order.java` is generated from `order.avsc` by `avro-maven-plugin` during the build, and the
generated sources are gitignored. Committing generated code invites the two artefacts to drift
apart; generating it makes the schema the single source of truth by construction.

---

## 3. The poison pill, and why the consumer deserialises by hand

This is the single most consequential decision in the implementation.

### 3.1 The trap

The natural way to write the consumer is:

```java
props.put(VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
// ...
ConsumerRecords<String, Order> records = consumer.poll(timeout);   // Order objects, ready to use
```

This works perfectly until a message arrives that is not valid Avro — a different team's service
publishing to the topic with the wrong serialiser, a corrupted write, a schema deleted from the
registry. Then:

1. `poll()` itself throws `SerializationException`, *before* returning any records.
2. There is no record object to inspect, log, or route to a DLQ — the failure happened below the
   application.
3. The offset is never advanced, because nothing was successfully processed.
4. The next `poll()` returns the same message and throws again.

The consumer is now wedged forever on one bad message, and every valid message behind it on that
partition is stalled indefinitely. Worse, the symptom — a consumer that appears to be running but
makes no progress — is genuinely hard to diagnose. This is the classic Kafka **poison pill**.

### 3.2 The fix

The consumer reads `byte[]` values, which cannot fail to deserialise, and calls the Avro
deserialiser itself inside the poll loop:

```java
props.put(VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
// ...
try {
    order = (Order) avroDeserializer.deserialize(record.topic(), record.value());
} catch (Exception e) {
    deadLetter(record, DlqReason.DESERIALIZATION_FAILED, e, 1);   // routed, not fatal
    return;
}
```

A decode failure becomes an ordinary error attached to a record the application is holding, and
is handled like any other. The cost is one cast and slightly more code; the benefit is that a
single malformed message can no longer halt a partition.

The producer deliberately injects poison pills through a **separate** raw-bytes producer, to make
the point that the corruption originates outside the application rather than from a bug in the
Avro path.

---

## 4. Classifying failures

Retry logic is only as good as the decision about *what* to retry. Two failure modes that look
identical to a `catch` block need opposite treatment:

| Class | Example | Why | Handling |
|---|---|---|---|
| **Transient** | gateway timeout, connection reset, HTTP 503 | The input is fine; the environment misbehaved. A later attempt may well succeed. | Retry with backoff |
| **Permanent** | negative price, blank `orderId`, undecodable bytes | The input itself is wrong. Every attempt fails identically. | Dead-letter immediately |

Getting this wrong is costly in both directions. Treating everything as transient means a
malformed record burns its entire retry budget, delaying every message behind it, and still ends
up in the DLQ. Treating everything as permanent means a two-second network blip discards
perfectly good orders.

`ErrorClassifier` walks the exception's cause chain, since the informative exception is often
wrapped. Anything unrecognised defaults to **permanent**. That bias is deliberate: an unknown
fault sent to the DLQ is visible and replayable, whereas an unknown fault retried forever
silently stalls a partition — the failure mode that is hardest to notice.

---

## 5. Retry strategy

### 5.1 What was implemented

In-process retry with **exponential backoff and jitter**, bounded at three attempts:

```
attempt 1  ->  fail  ->  wait ~200ms
attempt 2  ->  fail  ->  wait ~400ms
attempt 3  ->  fail  ->  orders.DLQ  (reason: RETRIES_EXHAUSTED)
```

**Why back off at all?** If a downstream service is failing because it is overloaded, retrying
immediately adds load to something already struggling. Backing off gives it room to recover.

**Why jitter?** Without it, every consumer that hit the same outage retries at exactly the same
instants. The recovering service is then hit by synchronised spikes — a thundering herd that can
knock it straight back down. Spreading each delay randomly over ±20% de-synchronises them.

**Why a bounded budget?** Unbounded retry is indistinguishable from a hang. Three attempts is
enough to ride out a brief blip; beyond that, something is genuinely wrong and the record belongs
somewhere a human will see it.

### 5.2 The cost: blocking retries consume the poll interval

Retrying in process means sleeping inside the poll loop, and Kafka is watching. If a consumer
does not call `poll()` within `max.poll.interval.ms`, the broker declares it dead and rebalances
its partitions to another member — which then reprocesses everything uncommitted, producing
duplicates.

The worst case is every record in a batch exhausting its budget:

```
max.poll.records x (maxAttempts - 1) x maxBackoff
      20         x         2         x   5000ms    = 200s
```

This is bounded well inside the configured `max.poll.interval.ms` of 600s. Both values are set
explicitly in `OrderConsumer` with this calculation as the justification — the interaction is
easy to miss and produces a confusing, intermittent failure when it is.

### 5.3 Alternatives considered

**Tiered retry topics.** Failed records are republished to `orders.retry.5s`, then
`orders.retry.30s`, each consumed by a delay-aware consumer, before finally reaching the DLQ.

- *Advantage:* non-blocking. The main consumer never sleeps, so the partition keeps moving and
  the `max.poll.interval.ms` tension disappears entirely. This is what a high-throughput
  production system generally does.
- *Cost:* two extra topics, two extra consumers, and per-message ordering is lost — a retried
  record is now processed after records that came behind it.
- *Rejected because:* at the rates involved here the blocking cost is negligible, and the extra
  moving parts are extra things to fail during a live demonstration. The trade-off inverts at
  production scale.

**Pause/resume.** Call `consumer.pause()` on the partition, keep polling (which returns nothing
but satisfies the liveness check), and `resume()` when the backoff elapses. This removes the poll
interval risk while keeping ordering, at the cost of noticeably more complex state handling. A
reasonable middle ground, and the natural next step if backoffs ever grew long.

**Infinite retry.** Rejected outright. It converts a bad message into a permanently stalled
partition, and provides no signal that anything is wrong.

---

## 6. Dead letter queue design

### 6.1 The payload is the original bytes, unmodified

The DLQ record's value is exactly what was consumed — not a JSON envelope, not a re-serialised
object.

The reason is decisive: a poison pill is by definition a message that could not be turned into an
object. Any format that requires understanding the payload cannot represent the very records you
most need to keep. Byte-identical payloads also mean a corrected message can be replayed straight
onto the source topic with no unwrapping step.

### 6.2 Diagnostics ride in headers

| Header | Purpose |
|---|---|
| `dlq.reason` | which of the four failure classes |
| `dlq.error.class` / `dlq.error.message` | what actually went wrong |
| `dlq.original.topic` / `.partition` / `.offset` | exactly where it came from |
| `dlq.original.timestamp` | when it was first produced |
| `dlq.attempts` | how hard we tried |
| `dlq.failed.at` | when we gave up |
| `dlq.consumer.group` | which consumer group gave up |

Headers from the original producer are copied across first, so upstream tracing context survives
and a dead-lettered record can still be correlated back to the request that created it.

### 6.3 The DLQ write is synchronous

`producer.send()` alone only queues a record into the client's buffer. If the consumer then
committed its offset and exited, the record could be lost from the source topic *and* never reach
the DLQ — the one outcome a dead letter queue exists to prevent. Blocking on the returned future
means the broker has acknowledged the record before the offset is committed.

The DLQ producer uses `acks=all` with idempotence enabled. If the DLQ write fails outright, the
consumer stops **without committing**, so the record is redelivered rather than lost.

### 6.4 A DLQ nobody reads is just a slow delete

`DlqViewer` prints the queue with full diagnostics, decoding records that decode and falling back
to a hex and ASCII dump for those that do not. It assigns partitions directly rather than joining
a consumer group, so it never consumes the queue or interferes with other readers, and can be run
as often as needed.

---

## 7. Delivery guarantees

**Producer:** `acks=all` plus `enable.idempotence=true`. The write is acknowledged only once all
in-sync replicas have it, and the broker de-duplicates internal retries, so a network hiccup
during send cannot silently produce the same order twice.

**Consumer:** `enable.auto.commit=false`. Offsets are committed only after every record in the
batch has reached a terminal state — processed, or durably dead-lettered. Auto-commit would
commit on a timer, potentially acknowledging records that were still in flight; a crash would
then lose them permanently.

This yields **at-least-once** delivery. A crash mid-batch causes redelivery of that batch, so a
record may be processed twice, but no record is ever silently dropped.

For a running average that is the right trade. A lost order corrupts the aggregate invisibly and
permanently; a duplicated order nudges it by a bounded, diminishing amount. Given the choice
between an average that is slightly off and one that is quietly wrong, the first is far
preferable.

Exactly-once would require the Kafka transactions API, tying offset commits and output writes
into a single atomic transaction. That is justified when the output is itself a Kafka topic; here
the output is an in-memory aggregate, so the complexity would not pay for itself.

---

## 8. Real-time aggregation

### 8.1 Welford's online algorithm

The obvious running average keeps a sum and a count. It works, but the sum grows without bound
while each new price stays small, so their floating-point exponents drift apart and low-order
bits are quietly lost — precisely the situation a long-running stream consumer sits in.

Welford's method updates the mean by the scaled residual of each new sample instead:

```
mean_n = mean_(n-1) + (x_n - mean_(n-1)) / n
```

Every term stays the same order of magnitude as the data, so accuracy holds over millions of
messages. Carrying the companion sum-of-squared-deviations term costs one extra multiply and
yields variance and standard deviation for free, which the dashboard uses to show price spread.
`RunningStatsTest` verifies this directly with a data set offset by 10⁹, where a naive running
sum measurably loses precision.

### 8.2 Three views, because one is misleading

- **Lifetime** — every order ever processed. This is the figure the brief asks for.
- **Per product** — reveals that `Item3` runs dearer than `Item1`, which the global mean hides.
- **60-second sliding window** — the genuinely real-time view.

The third exists because the lifetime average has a presentational flaw: after a few thousand
messages it converges and stops visibly moving, so a screen showing only that number looks
frozen, and a real shift in the stream would be invisible behind the weight of history. The
windowed average reacts immediately. `PriceAggregatorTest` demonstrates the difference — after a
price jump, the lifetime average moves from 10.0 to 14.85 while the window reads 500.0.

The aggregator takes the current time as a parameter rather than reading the clock internally,
which makes window eviction fully deterministic under test: no sleeping, no flakiness.

---

## 9. Testing

74 unit tests, none requiring a running broker. Two techniques make that possible:

**Injected randomness.** Retry jitter and fault injection are driven by a `RandomGenerator` passed
in by the caller. Tests supply a scripted sequence, turning "fails roughly 15% of the time" into
"fails on exactly this call", so assertions are exact rather than statistical.

**Kafka's `MockProducer`.** `DeadLetterPublisherTest` asserts on real `ProducerRecord` objects —
every header, and byte-for-byte payload preservation — with no broker involved.

The tests that carry the most weight are the ones pinning down behaviour that is easy to get
subtly wrong: the backoff overflow guard at absurd attempt counts, sliding-window eviction
boundaries, cause-chain unwrapping in the classifier, and the exclusion of never-retried records
from the retry success rate.

---

## 10. Limitations and future work

**Single consumer instance.** The design is partition-parallel — three partitions on `orders`
means three consumers could share the load — but aggregation state is per-process, so each would
compute its own partial average. Correct global aggregation across instances would need Kafka
Streams with a state store, or an external store.

**Aggregation state is in memory.** Restarting the consumer resets the running average. Because
offsets are committed, it does not re-read history. Kafka Streams with a changelog-backed state
store would make the aggregate durable and recoverable.

**No automated DLQ replay.** Records are inspectable and byte-identical, so replay is
straightforward, but the tool to do it is not written. It would be a small addition: read
`orders.DLQ`, filter by `dlq.reason`, republish to `orders`.

**Schema evolution is configured but not exercised.** The registry enforces `BACKWARD`
compatibility, but the assignment's schema is fixed at three fields so no second version exists.
Adding, say, an optional `currency` field with a default would demonstrate old consumers reading
new messages unchanged.

**Simulated downstream.** Transient failures come from a probability check, not a real service.
This makes the demo deterministic and repeatable, but it does not exercise real network
behaviour — connection pool exhaustion, partial reads, TCP-level timeouts.

---

## 11. Summary

| Requirement | How it is met |
|---|---|
| Avro serialisation | Schema Registry with the Confluent wire format; `Order.java` generated from `order.avsc` at build time |
| Real-time aggregation | Welford's online algorithm, tracked lifetime, per product, and over a 60-second sliding window |
| Retry logic | Exponential backoff with jitter, bounded budget, applied only to failures classified as transient |
| Dead letter queue | `orders.DLQ` holding original bytes plus ten diagnostic headers, written synchronously before the offset is committed |
| Live demonstration | Single-command scripted demo with a live terminal dashboard and a DLQ inspector |

The parts worth defending are the ones where the obvious implementation is wrong: deserialising
Avro by hand so a poison pill cannot wedge a partition, refusing to retry failures that can never
succeed, keeping the DLQ payload byte-identical so undecodable records survive intact, and
committing offsets only after a record is genuinely finished with.
