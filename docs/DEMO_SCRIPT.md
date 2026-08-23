# Live Demo Runbook

A step-by-step script for demonstrating the system. Each step lists the command, what to point
at, and what should appear — so nothing has to be improvised in front of an audience.

**Total time:** about 8 minutes, or 4 if you skip the optional steps.

---

## Before the demo (do this the day before, not on the day)

> The first `docker compose up` downloads roughly 1 GB of images. On a slow connection that
> takes many minutes. Do it in advance — this is the single most likely way a live demo goes
> wrong.

```powershell
.\scripts\up.ps1        # pulls images, starts the stack, waits until healthy
.\scripts\build.ps1     # downloads Maven + dependencies, runs the tests, builds the jar
.\scripts\down.ps1      # stop it again; the images and jar are now cached locally
```

Checklist:

- [ ] Docker Desktop starts and reports **Running**
- [ ] `.\scripts\up.ps1` finishes with **Ready**
- [ ] `.\scripts\build.ps1` reports **BUILD SUCCESS** with 74 tests passing
- [ ] <http://localhost:8080> loads Kafka UI
- [ ] Terminal font size is large enough to read from the back of the room

---

## Setup

Open **three** PowerShell terminals in the project root, ideally side by side:

| Terminal | Role |
|---|---|
| **A** | Consumer — the live dashboard |
| **B** | Producer — the order stream |
| **C** | Commands, DLQ inspection |

Also open a browser at <http://localhost:8080>.

---

## Step 1 — Start the infrastructure *(Terminal C)*

```powershell
.\scripts\up.ps1
```

**Say:** "This brings up a single-node Kafka broker in KRaft mode — no ZooKeeper — plus the
Confluent Schema Registry and a web UI. The script waits for the healthchecks, so it only
returns once the broker is genuinely ready to serve."

**Expect:** a topic list showing `orders` and `orders.DLQ`, then a green **Ready**.

---

## Step 2 — Show the schema *(browser)*

Open **Kafka UI → Topics**. Point out `orders` (3 partitions) and `orders.DLQ`.

**Say:** "The `orders` topic has three partitions, so the workload can be shared across three
consumers. The DLQ has one, with a week's retention — failed messages need to survive long
enough for someone to look at them."

Show [`src/main/avro/order.avsc`](../src/main/avro/order.avsc):

**Say:** "This is the schema from the brief — `orderId`, `product`, `price`. `Order.java` is
generated from this file at build time, so the schema is the single source of truth and the
generated code is never committed."

---

## Step 3 — Start the consumer *(Terminal A)*

```powershell
.\scripts\run-consumer.ps1
```

**Expect:** the banner, then the dashboard with zeros and *"waiting for orders..."*.

**Say:** "The consumer is now subscribed and idle. Note the retry policy in the banner: three
attempts, 200ms doubling, plus or minus 20% jitter."

---

## Step 4 — Start the producer *(Terminal B)*

```powershell
.\scripts\run-producer.ps1
```

**Expect:** lines scrolling in Terminal B —

```
  ok       key=1001   Item4       317.44
  ok       key=1002   Item1        88.10
  INVALID  key=1003   Item2      -412.55   <- NEGATIVE_PRICE
  POISON   key=1004   61 bytes of non-Avro data
```

**Say:** "The producer is streaming randomised orders, and deliberately injecting two kinds of
fault — invalid orders that are valid Avro but break a business rule, and poison pills that
aren't Avro at all. A system that only ever sees clean data proves nothing about its error
handling."

---

## Step 5 — The running average *(Terminal A)* — **the core requirement**

Let it run for 20–30 seconds.

**Point at the big green number.**

**Say:** "That's the running average price, updating in real time. It's computed with Welford's
online algorithm rather than a running sum divided by a count — the sum grows without bound while
each price stays small, so a naive version quietly loses floating-point precision over a
long-lived stream. Welford's keeps every term the same magnitude as the data, and gives us the
standard deviation for free."

**Point at `last 60s`.**

**Say:** "Two averages are shown deliberately. The lifetime figure converges and then barely
moves, so on its own it looks frozen. The 60-second window reacts immediately — that's what
real-time aggregation actually means."

**Point at the per-product bars.** "Each product tracked separately, which the global mean hides."

---

## Step 6 — Retries *(Terminal A)* — **requirement two**

**Point at RECENT EVENTS**, where yellow `RETRY` lines appear followed by green `RECOVERED` ones:

```
  RETRY     order 1187 attempt 1/3 failed, retrying in 214ms - payment gateway did not respond
  RECOVERED order 1187 succeeded on attempt 2/3
```

**Say:** "The consumer simulates a downstream outage on about 15% of orders. Those are classified
as *transient*, so they're retried with exponential backoff — 200ms, then 400ms, with jitter. The
jitter matters: without it every consumer that hit the same outage would retry at exactly the
same instant and hammer a service that's already struggling."

**Point at `recovered ... (81%)`.** "Most retried orders succeed on a later attempt. That's the
retry logic earning its place."

---

## Step 7 — The dead letter queue *(Terminal A)* — **requirement three**

**Point at the RELIABILITY line**, at the breakdown `validation · poison · exhausted`.

**Say:** "Three distinct reasons, and the distinction is the whole point. A negative price is
*permanent* — retrying it accomplishes nothing except stalling every message behind it — so it
goes straight to the DLQ with no retries at all. Poison pills likewise. Only records that failed
in a way that looked transient are retried, and they reach the DLQ only after exhausting the
budget."

---

## Step 8 — Inspect the DLQ *(Terminal C)*

```powershell
.\scripts\run-dlq-viewer.ps1
```

**Expect:**

```
  DLQ offset 12       key 1043        2026-08-23T20:14:09.412Z
    reason      : VALIDATION_FAILED
    attempts    : 1
    origin      : orders[2] offset 903
    error       : com.assignment.orders.exception.PermanentProcessingException
                  price must be greater than zero, got -412.55 for order 1043
    payload     : orderId=1043 product=Item2 price=-412.55
```

**Say:** "Every failed message carries the reason, the attempt count, and its exact origin —
topic, partition and offset — so it can be found again or replayed once the bug is fixed."

**Scroll to a `DESERIALIZATION_FAILED` entry**, showing the hex dump:

**Say:** "This one couldn't be decoded, so the viewer falls back to a hex dump. That's *why* the
DLQ stores the original bytes untouched rather than a JSON envelope — a poison pill is by
definition a message you couldn't turn into an object, so any format that requires understanding
the payload can't represent the records you most need to keep."

---

## Step 9 — The poison-pill design point *(optional, but the strongest technical point)*

Open [`OrderConsumer.java`](../src/main/java/com/assignment/orders/consumer/OrderConsumer.java)
at the class comment.

**Say:** "The obvious way to write this consumer is to set `KafkaAvroDeserializer` as the value
deserialiser and get `Order` objects straight from `poll()`. That works until a message arrives
that isn't valid Avro — then `poll()` itself throws, before returning any records. There's no
record to route to a DLQ, and the offset never advances, so the next poll returns the same
message and throws again. One malformed message stalls the partition permanently.

So this consumer reads raw bytes, which can't fail to deserialise, and calls the Avro
deserialiser itself inside a try/catch. A decode failure becomes an ordinary error on a record
we're holding, and can be dead-lettered like anything else."

---

## Step 10 — Delivery guarantees: kill and restart *(optional)*

In **Terminal A**, press `Ctrl+C`. Note the final totals. Then:

```powershell
.\scripts\run-consumer.ps1
```

**Say:** "Auto-commit is off — offsets are committed only after every record in a batch is
finished with, either processed or durably written to the DLQ. So it resumes exactly where it
left off, with nothing lost. That's at-least-once: a crash mid-batch could reprocess a few
records, but nothing is ever silently dropped. For an aggregate that's the right trade — a lost
order corrupts the average invisibly and permanently, while a duplicate nudges it by a bounded
amount."

---

## Step 11 — Force the DLQ *(optional, dramatic)*

Stop the consumer (`Ctrl+C`), then:

```powershell
.\scripts\run-consumer.ps1 -FailureRate 1.0
```

**Say:** "Now every single order hits a simulated outage. Watch every record burn all three
attempts and land in the DLQ under `RETRIES_EXHAUSTED` — the retry budget doing its job as a
circuit breaker rather than retrying forever."

**Expect:** the `exhausted` counter climbing steadily while `processed` stays flat.

Stop it and restart normally afterwards.

---

## Step 12 — Tests *(optional)*

```powershell
.\mvnw.cmd test
```

**Say:** "74 unit tests, none of which need a running broker. Randomness is injected, so retry
behaviour is asserted exactly rather than statistically, and the DLQ publisher is tested against
Kafka's `MockProducer` — asserting every header and byte-for-byte payload preservation."

---

## Wrap up

```powershell
.\scripts\down.ps1
```

**Closing line:** "Four requirements — Avro serialisation, real-time aggregation, retry logic,
and a dead letter queue. The interesting parts were the places where the obvious implementation
is subtly wrong: deserialising by hand so a poison pill can't wedge a partition, refusing to
retry failures that can never succeed, and committing offsets only once a record is genuinely
finished with."

---

## If something goes wrong

| Symptom | Fix |
|---|---|
| `Docker is not running` | Start Docker Desktop, wait for **Running**, re-run `.\scripts\up.ps1` |
| Stack never becomes healthy | `docker compose -f docker/docker-compose.yml logs kafka schema-registry` |
| Consumer dashboard stays empty | The group already consumed the topic. Use `.\scripts\run-consumer.ps1 -Group demo2` |
| Dashboard shows escape sequences | `.\scripts\run-consumer.ps1 -NoDashboard` |
| Port already in use | `netstat -ano \| findstr "9092"`, stop the offending process |
| Everything is confused | `.\scripts\down.ps1 -Purge` then `.\scripts\up.ps1` — full reset |

**If the live demo fails entirely:** the DLQ viewer and the test suite both run without a
broker being healthy, and `docs/REPORT.md` carries the full design reasoning.
