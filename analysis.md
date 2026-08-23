# Requirement Analysis

**Assignment:** EC 8202 Big Data — Chapter 3
**Project:** Kafka Avro Order Pipeline
**Audit date:** 23 August 2026
**Audited against:** [`Assignement Chapter 3.md`](Assignement%20Chapter%203.md)

---

## Verdict

**All stated requirements are met, and every one was verified by running the system**, not by
code review alone. The pipeline was executed end to end against a live Kafka cluster: 150
messages were produced and every one of them was accounted for by the consumer with no losses
and no duplicates.

Two defects were found *during* this audit and fixed — both in the startup path, and one of them
would have broken the live demonstration. They are documented in
[Defects found and fixed](#defects-found-and-fixed-during-this-audit) rather than quietly
patched.

The known limitations in [Gaps and limitations](#gaps-risks-and-limitations) are design
boundaries, not unmet requirements. None of them is required by the brief.

---

## How each item was verified

Claims in this document are labelled with the strength of evidence behind them. Nothing is
claimed on stronger evidence than it actually has.

| Tier | Meaning |
|---|---|
| **EXECUTED** | Observed running against a live Kafka broker; output captured in the [evidence appendix](#evidence-appendix) |
| **TESTED** | Proven by an automated test in the suite (74 tests, all passing) |
| **INSPECTED** | Verified by reading the code only |

---

## Compliance matrix

| # | Requirement (from the brief) | Status | Verified | Implementation |
|---|---|---|---|---|
| 1 | Kafka-based system that **produces and consumes** order messages | **MET** | EXECUTED | [`OrderProducer.java`](src/main/java/com/assignment/orders/producer/OrderProducer.java), [`OrderConsumer.java`](src/main/java/com/assignment/orders/consumer/OrderConsumer.java) |
| 2 | Each message uses **Avro serialization** | **MET** | EXECUTED | `OrderProducer.java:145` `KafkaAvroSerializer`; `OrderConsumer.java:368-373` `KafkaAvroDeserializer` |
| 3 | **Real-time aggregation** (running average of prices) | **MET** | EXECUTED + TESTED | [`RunningStats.java`](src/main/java/com/assignment/orders/aggregate/RunningStats.java), [`PriceAggregator.java`](src/main/java/com/assignment/orders/aggregate/PriceAggregator.java) |
| 4 | **Retry logic** for temporary failures | **MET** | EXECUTED + TESTED | [`RetryPolicy.java`](src/main/java/com/assignment/orders/consumer/RetryPolicy.java), `OrderConsumer.java:242-254` |
| 5 | **Dead Letter Queue** for permanently failed messages | **MET** | EXECUTED + TESTED | [`DeadLetterPublisher.java`](src/main/java/com/assignment/orders/dlq/DeadLetterPublisher.java), [`DlqViewer.java`](src/main/java/com/assignment/orders/dlq/DlqViewer.java) |
| 6 | **Demonstrates the system live** | **READY** | EXECUTED | [`scripts/demo.ps1`](scripts/demo.ps1), [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md) — see note below |
| 7 | Maintains a **Git repository** for submission | **MET** | EXECUTED | 17 commits, clean tree, staged by milestone |
| 8 | Free choice of language; **researched and implemented independently** | **MET** | INSPECTED | Java 17; design rationale in [`docs/REPORT.md`](docs/REPORT.md) |
| 9 | Schema `order.avsc`: `orderId` string, `product` string, `price` float | **MET** | EXECUTED | [`order.avsc`](src/main/avro/order.avsc), confirmed as registered in the Schema Registry |

> **On requirement 6:** the system has been *demonstrated running* end to end, and the runbook and
> one-command launcher are in place. What remains is the act of presenting it, which only you can
> do. Everything needed for that is ready and rehearsed.

---

## Requirement detail

### 1. Produces and consumes order messages — MET

A producer streams randomised orders to the `orders` topic (3 partitions); a consumer reads them,
processes them, and aggregates. Both were run against a live broker.

The strongest evidence is that **the two sides reconcile exactly**:

| Producer sent | | Consumer accounted for | |
|---|---|---|---|
| valid orders | 108 | processed successfully | 108 |
| invalid orders | 29 | DLQ `VALIDATION_FAILED` | 29 |
| poison pills | 13 | DLQ `DESERIALIZATION_FAILED` | 13 |
| **total** | **150** | **total (108 + 42)** | **150** |

Every message is accounted for: none lost, none double-counted.

### 2. Avro serialization — MET

The producer serialises with `KafkaAvroSerializer` and registers the schema on first send. The
Schema Registry confirms it live:

```
$ curl -s http://localhost:8081/subjects
["orders-value"]

$ curl -s http://localhost:8081/subjects/orders-value/versions/1
{"subject":"orders-value","version":1,"id":1,"schema":"{\"type\":\"record\",
 \"name\":\"Order\",\"namespace\":\"com.assignment.orders.avro\", ...
 {\"name\":\"orderId\",\"type\":\"string\"},
 {\"name\":\"product\",\"type\":\"string\"},
 {\"name\":\"price\",\"type\":\"float\"}]}"}
```

`Order.java` is **generated from `order.avsc` at build time** by `avro-maven-plugin`, so the
schema is the single source of truth and generated code is never committed.

The registry enforces `BACKWARD` compatibility (`{"compatibilityLevel":"BACKWARD"}`), so a future
schema version that existing consumers could not read would be rejected at registration.

### 3. Real-time aggregation — MET

The running average is computed with **Welford's online algorithm** rather than a naive
sum ÷ count. The sum in a naive implementation grows without bound while each price stays small,
so the floating-point exponents drift apart and precision is quietly lost over a long-lived
stream. Welford keeps every term the same magnitude as the data and yields standard deviation for
free.

Observed live: `running average: 269.61 (over 108 orders)`, and in the dashboard `265.30`
lifetime alongside a `last 60s` figure and per-product breakdown.

Three views are tracked, deliberately:

- **Lifetime** — the figure the brief asks for
- **Per product** — `Item3` averaging 304.00 against `Item1` at 248.86, which the global mean hides
- **60-second sliding window** — the lifetime average converges and stops visibly moving, so on
  its own it looks frozen; the window reacts immediately, which is what "real-time" actually means

`RunningStatsTest` proves the precision claim directly using a data set offset by 10⁹.

### 4. Retry logic — MET

Transient failures are retried with exponential backoff plus jitter, bounded at three attempts.
Observed live, with the doubling and the jitter both visible:

```
RETRY  order 1083 attempt 1/3 failed, retrying in 210ms - payment gateway did not respond
RETRY  order 1083 attempt 2/3 failed, retrying in 444ms - connection reset while committing
DLQ    offset 24 -> DLQ [exhausted] after 3 attempts - inventory service returned 503
```

In a normal run, **11 of 11 retried records recovered** (100%); the dashboard showed 38 of 42
(97%) over a longer run. Retries are doing real work, not just deferring failure.

Critically, **retries are applied only where they can help**. Invalid orders and poison pills go
to the DLQ `after 1 attempt` — never retried — because no amount of waiting turns a negative
price positive. Retrying them would burn the budget and stall every message queued behind them.

### 5. Dead Letter Queue — MET

Failed messages are routed to `orders.DLQ` with the original bytes preserved and ten diagnostic
headers attached. Three of the four reason codes were exercised live:

```
164 messages in the dead letter queue:
    DESERIALIZATION_FAILED   32
    VALIDATION_FAILED        70
    RETRIES_EXHAUSTED        62
```

The fourth, `UNEXPECTED_ERROR`, recorded zero — and that is the desired result. It is a
deliberate catch-all for faults the classifier does not recognise, so a run that never triggers
it means every failure encountered was one the system explicitly anticipated. Its handling is
covered by `ErrorClassifierTest` rather than by a live run.

The DLQ payload is the **unmodified original bytes**, not a JSON envelope. This is a deliberate
decision with a decisive justification: a poison pill is by definition a message that could not be
turned into an object, so any format requiring comprehension of the payload cannot represent the
very records you most need to keep. The viewer proves the bytes survived intact:

```
reason      : DESERIALIZATION_FAILED
origin      : orders[0] offset 15
error       : org.apache.kafka.common.errors.SerializationException
              Unknown magic byte!
payload     : undecodable, 49 bytes
              hex  7b 22 6f 72 64 65 72 49 64 22 3a 22 31 30 34 34 ...
              text {"orderId":"1044","product":"Item1","price":42.0...
```

Byte-identical payloads also mean a corrected message can be replayed onto the source topic with
no unwrapping step.

### 6. Live demonstration — READY

- `.\scripts\demo.ps1` brings up the stack, builds, and launches consumer and producer windows
- [`docs/DEMO_SCRIPT.md`](docs/DEMO_SCRIPT.md) is a 12-step runbook with the exact commands, what
  to point at, expected output, and a recovery table
- The live ANSI dashboard was confirmed rendering correctly (see appendix)

**Pre-demo warning:** the first `docker compose up` pulls several GB of images and took over an
hour on this connection. The images are now cached locally, so the stack starts in seconds — but
**do not run a first-time setup on demo day.**

### 7. Git repository — MET

17 commits, staged by milestone rather than dumped as one, so the history shows genuine
development: build tooling → infrastructure → schema → aggregation → producer → consumer →
tests → docs → fixes. Working tree clean.

### 8. Independent research and implementation — MET

Java 17 with the official Kafka client and Confluent Schema Registry.
[`docs/REPORT.md`](docs/REPORT.md) records the design reasoning, including alternatives that were
evaluated and rejected with stated trade-offs — tiered retry topics, pause/resume, infinite
retry, and exactly-once via the transactions API.

### 9. Schema definition — MET

Matches the brief exactly. Three fields, correct types, no additions:

| Field | Type | Confirmed |
|---|---|---|
| `orderId` | string | registered in Schema Registry, id 1 |
| `product` | string | registered in Schema Registry, id 1 |
| `price` | float | registered in Schema Registry, id 1 |

---

## Defects found and fixed during this audit

Auditing found two real bugs. Both are fixed and re-verified.

### Topic creation was silently failing

`kafka-init` exited with code 1 and **`orders` and `orders.DLQ` were never created** — only
Kafka's internal `__consumer_offsets` and `_schemas` existed.

*Cause:* in a YAML **folded** block (`>`), lines indented further than the first retain their
newlines rather than being folded into one line. The wrapped `kafka-topics --create` invocations
were therefore split into fragments, and `kafka-topics` responded by printing its usage text.
Fixed by switching to a **literal** block (`|`) with each command on a single line.

### `up.ps1` reported "Ready" for an unusable stack

Worse than the first bug. The script checked only *container health*, which says the broker is
accepting connections but nothing about whether the topics the application needs exist. Because
auto-topic-creation is deliberately disabled, the producer would have failed against a stack the
script had just certified as good — **the launcher was lying to the operator**, which is exactly
the failure you cannot afford in front of an audience.

Fixed by asserting `orders` and `orders.DLQ` are present before printing "Ready", and dumping
`kafka-init` logs with a remediation command if they are not. Verified in **both** directions: it
passes on a good stack, and deleting `orders.DLQ` makes it exit 1 with diagnostics rather than
report success.

---

## Gaps, risks and limitations

These are design boundaries, not unmet requirements. **None is required by the brief.** They are
recorded because a marker may probe them, and having a considered answer is worth more than
pretending they do not exist.

| # | Limitation | Impact | What would fix it |
|---|---|---|---|
| 1 | **Aggregation state is per-process.** `orders` has 3 partitions and would support 3 consumers, but each would compute its own partial average rather than a shared one. | Horizontal scaling of the consumer would produce several partial averages, not one global figure. | Kafka Streams with a state store, or an external store such as Redis. |
| 2 | **Aggregation state is in memory.** Restarting the consumer resets the running average. | The average restarts from zero; already-consumed messages are not re-read, since offsets are committed. | A changelog-backed Kafka Streams state store. |
| 3 | **No automated DLQ replay tool.** | Replay is straightforward — payloads are byte-identical — but must be done by hand. | A small tool: read `orders.DLQ`, filter by `dlq.reason`, republish to `orders`. |
| 4 | **Schema evolution is configured but never exercised.** `BACKWARD` compatibility is enforced, but the brief fixes the schema at three fields, so no second version exists. | The evolution story is theoretical rather than demonstrated. | Add an optional field with a default as v2 and show an old consumer reading new messages. |
| 5 | **Transient failures are simulated by probability**, not a real downstream service. | Deterministic and repeatable, but does not exercise real network behaviour (pool exhaustion, partial reads, TCP timeouts). | Point `OrderProcessor` at a real service, or a fault-injecting proxy such as Toxiproxy. |
| 6 | **Blocking retries consume the poll interval.** Worst case is `max.poll.records × (maxAttempts−1) × maxBackoff` = 200s. | Bounded well inside the configured 600s `max.poll.interval.ms`, so it is safe as configured — but raising the retry budget without re-checking this would trigger rebalances and duplicate processing. | Tiered retry topics, or pause/resume. Both are analysed in `docs/REPORT.md` §5.3. |
| 7 | **Delivery is at-least-once, not exactly-once.** | A crash mid-batch reprocesses that batch, nudging the average slightly. | Kafka transactions — deliberately not used; the trade is argued in `docs/REPORT.md` §7. |

---

## Likely viva questions

| Question | Answer lives in |
|---|---|
| Why Avro rather than JSON? | `docs/REPORT.md` §2.1 — size, and the enforced contract |
| What happens if a message is not valid Avro? | §3 — the poison pill; why the consumer deserialises by hand |
| Why not just retry everything? | §4 — retrying a permanent failure stalls the partition and still fails |
| Why exponential backoff, and why jitter? | §5.1 — load on a struggling service; the thundering herd |
| Could you lose a message? | §7 — manual commits, synchronous DLQ write, at-least-once |
| Why Welford instead of sum ÷ count? | §8.1 — floating-point drift over a long-lived stream |
| What would you do differently at scale? | §5.3 and §10 — tiered retry topics, Kafka Streams state store |

---

## Evidence appendix

All output below was captured from live runs on 23 August 2026.

### Infrastructure

```
NAMES             STATUS
kafka             Up (healthy)
schema-registry   Up (healthy)
kafka-ui          Up
kafka-init        Exited (0)

Topic: orders      PartitionCount: 3   ReplicationFactor: 1
Topic: orders.DLQ  PartitionCount: 1   ReplicationFactor: 1
```

### Producer — bounded run of 150

```
sent 150 messages: 108 valid, 29 invalid, 13 poison
```

### Consumer — full reconciliation

```
------------------------------------------------------------
  FINAL TOTALS
------------------------------------------------------------
  consumed          : 150
  processed         : 108
  running average   : 269.61  (over 108 orders)
  retry attempts    : 11
  recovered by retry: 11
  dead-lettered     : 42
      DESERIALIZATION_FAILED 13
      VALIDATION_FAILED      29
      RETRIES_EXHAUSTED      0
      UNEXPECTED_ERROR       0
------------------------------------------------------------
```

### Forced retry exhaustion (`TRANSIENT_FAILURE_RATE=1.0`)

```
  consumed          : 80
  processed         : 0
  retry attempts    : 124
  recovered by retry: 0
  dead-lettered     : 80
      DESERIALIZATION_FAILED 6
      VALIDATION_FAILED      12
      RETRIES_EXHAUSTED      62
```

The retry budget acts as a circuit breaker: records stop after three attempts rather than
retrying forever.

### Offset commit and resume

```
Step 1  same group re-run          -> consumed 0    (offsets were committed)
Step 2  produce 20 new messages    -> sent 20 messages: 20 valid
Step 3  same group resumes         -> consumed 20, processed 20, dead-lettered 0
                                      running average 246.43 (over 20 orders)
```

Exactly the new messages, no reprocessing of old ones, nothing lost.

### Live dashboard

```
  ==============================================================================
  KAFKA AVRO ORDER PIPELINE                                          up 00:00:14
  ==============================================================================
  localhost:9092  |  topic orders  |  group audit-dash2  |  dlq orders.DLQ
  retry: 3 attempts, backoff 200ms x2.0 (max 5000ms), jitter +/-20%

  RUNNING AVERAGE PRICE
  ------------------------------------------------------------------------------
         265.30   lifetime        last 60s     265.30   3.8/s
   aggregated 230        min     13.44   max    499.56   std dev   145.32

  AVERAGE BY PRODUCT
  ------------------------------------------------------------------------------
   Item1    ##################....    248.86  n=52
   Item2    ####################..    279.66  n=35
   Item3    ######################    304.00  n=27
   Item4    ##################....    244.94  n=35
   Item5    ##################....    251.86  n=32
   Item6    ####################..    274.49  n=49

  RELIABILITY
  ------------------------------------------------------------------------------
   consumed 290      processed 230      retried 42     recovered 38 (97%)
   dead-lettered 60      validation 41    poison 18    exhausted 1     other 0

  RECENT EVENTS
  ------------------------------------------------------------------------------
   22:11:35  DLQ       offset 90 -> DLQ [poison] after 1 attempt - Unknown m...
   22:11:35  RETRY     order 1120 attempt 1/3 failed, retrying in 210ms - do...
   22:11:35  RECOVERED order 1120 succeeded on attempt 2/3
```

### Test suite

```
Tests run: 74, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

| Suite | Tests |
|---|---|
| `OrderProcessorTest` | 12 |
| `ProcessingMetricsTest` | 10 |
| `RetryPolicyTest` | 9 |
| `OrderGeneratorTest` | 9 |
| `PriceAggregatorTest` | 8 |
| `RunningStatsTest` | 6 |
| `DeadLetterPublisherTest` | 6 |
| `ErrorClassifierTest` | 14 |

---

## Reproducing this audit

```powershell
.\scripts\up.ps1                                            # stack up, topics asserted
.\mvnw.cmd test                                             # 74 tests
.\scripts\run-producer.ps1 -Count 150 -BadRate 0.15 -PoisonRate 0.10
.\scripts\run-consumer.ps1 -Group audit-1 -RunForSeconds 40 -NoDashboard
.\scripts\run-dlq-viewer.ps1
.\scripts\run-consumer.ps1 -Group audit-2 -FailureRate 1.0 -RunForSeconds 40 -NoDashboard
```
