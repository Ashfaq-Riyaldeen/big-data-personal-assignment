# Live Demonstration Script

Target length **4 minutes 30 seconds**, leaving a safety margin under the five-minute limit. The
video shows evidence from the real running system, not slides: Swagger UI to send orders, Kafka UI
to inspect what landed on the topics, and the consumer terminal to show the processing result.

## Before recording

Do all of this first. Nothing in the video should be a download, a build, or a wait.

- [ ] Pull the Docker images so no download happens on camera: `docker compose pull`.
- [ ] Build once: `./mvnw clean install`.
- [ ] Reset to a clean state so the running average starts from zero:
      `docker compose down -v && docker compose up -d`, then `./scripts/verify-stack.sh`.
- [ ] Start the producer and the consumer **after** the reset, each in its own terminal. A producer
      left running through a reset keeps a stale schema id and the consumer cannot read its output.
- [ ] Confirm `GET http://localhost:8082/api/stats` returns zeros.
- [ ] Clear both terminals. Increase the terminal font so a line is readable at 1080p; the widest
      log line is about 230 characters, so use a wide window.
- [ ] Open these tabs in this order, and load each one fully:
  1. Swagger UI — `http://localhost:8080/swagger-ui/index.html`
  2. Kafka UI topics — `http://localhost:8090/ui/clusters/local-kafka/all-topics`
  3. Kafka UI `orders.v1` messages, with the value serde set to **SchemaRegistry**
  4. Kafka UI Schema Registry — `orders.v1-value`
  5. Kafka UI `orders.v1-dlt` messages, value serde **SchemaRegistry**
  6. Kafka UI consumer group `orders-consumer-v1` — **this page takes around 20 seconds to
     load**, so open it now, not during the recording
  7. `order.avsc` in the editor
  8. The GitHub repository
- [ ] Disable notifications. Close everything unrelated.
- [ ] Do one full practice run and time it.

## Running order

| Time | Screen | Say and do | Evidence on screen |
| --- | --- | --- | --- |
| 0:00 – 0:20 | README architecture diagram | Introduce the project: a Kafka producer and consumer for Avro order messages with a running average, retry, and a dead letter queue. | The diagram |
| 0:20 – 0:40 | Kafka UI topics | The stack is running: one broker in KRaft mode, four topics, no messages yet. | Topic list, message counts 0 |
| 0:40 – 1:00 | `order.avsc`, then Schema Registry tab | The schema from the brief — `orderId`, `product`, `price` as a float. It is registered in the Schema Registry and the Java model is generated from it at build time. | Schema file, then subject `orders.v1-value` |
| 1:00 – 1:45 | Swagger UI, then consumer terminal | Send `1001 / Item1 / 100.0` with `POST /api/orders`. Show `[AVERAGE] runningAverage=100.00`. Send `1002 / Item2 / 300.0`. Show `200.00`. | `[SUCCESS]` and `[AVERAGE]` lines |
| 1:45 – 2:00 | Kafka UI `orders.v1` messages | Refresh. The two orders, key `1001` and `1002`, values decoded — they are real Avro records, readable because Kafka UI resolves the schema by id. | Decoded JSON values |
| 2:00 – 2:50 | Swagger UI, then consumer terminal | `POST /api/orders/demo/temporary` — `TEMP_FAIL` at 500. Watch the terminal: `[RETRY] attempt=1`, two seconds, `[RETRY] attempt=2` arriving from `orders.v1-retry-0`, two seconds, `[SUCCESS] attempt=3` from `orders.v1-retry-1`. Then `[AVERAGE] count=3 ... 300.00` — counted once, not three times. | Timestamps showing the gaps; `topic=` changing per attempt |
| 2:50 – 3:35 | Swagger UI, consumer terminal, then Kafka UI DLQ tab | `POST /api/orders/demo/permanent` — price −10. `[PERMANENT-ERROR]` immediately, then `[DLQ]`, then `[AVERAGE] unchanged at 300.00`. No retry. In Kafka UI, expand the `1004` record and open its **Headers** tab: `kafka_original-topic`, `kafka_exception-cause-fqcn`. | The headers, the unchanged average |
| 3:35 – 4:00 | Kafka UI consumer group | Partition 0 assigned, current offset, lag 0 — the consumer is keeping up. Then `GET /api/stats` in the browser: `successfulOrders 3, totalPrice 900, runningAverage 300`. | Lag 0; the stats JSON |
| 4:00 – 4:30 | GitHub repository | The README, the test folders, the commit history. Close by restating the four requirements and where each is proven. | Repository page |

The `ALWAYS_FAIL` case is covered by the automated tests and the README evidence. Include it in the
recording only if there is time to spare; it adds about 15 seconds.

## Exact requests

Use the demo endpoints, which carry the fixed values, so nothing has to be typed on camera except
the first two orders.

| Step | Endpoint | Body | Expected |
| --- | --- | --- | --- |
| 1 | `POST /api/orders` | `{"orderId":"1001","product":"Item1","price":100.0}` | average 100.00 |
| 2 | `POST /api/orders` | `{"orderId":"1002","product":"Item2","price":300.0}` | average 200.00 |
| 3 | `POST /api/orders/demo/temporary` | — | 2 retries, success on attempt 3, average 300.00 |
| 4 | `POST /api/orders/demo/permanent` | — | DLQ, average unchanged |
| 5 | `POST /api/orders/demo/always-fail` | — | 3 attempts, DLQ, average unchanged |

If a take has to be repeated, either reset the stack and restart both services, or use the
`?orderId=` override on the demo endpoints with fresh identifiers — the running average will then
continue from where it was, which is fine as long as you narrate it.

## Narration

> This is a Kafka-based order processing system. The producer converts order requests into Avro
> messages, registered with the Schema Registry, and publishes them to the orders topic. The
> consumer deserialises each order, processes it, and maintains a running average of prices —
> updated only after an order succeeds. Temporary failures are retried up to three attempts across
> dedicated retry topics with a two-second backoff. Permanent failures, and retries that are
> exhausted, go to the dead letter queue with the original message and the error attached as
> headers. I will show normal processing, a temporary failure that recovers, and a permanent failure,
> and the average will stay correct throughout.

## What not to do

- Do not restart the consumer mid-recording: the running average is in memory and would reset.
- Do not record a build, an image pull, or a page loading.
- Do not rely on random data; the demo endpoints exist so every take produces the same numbers.
- Do not explain every source file. Show the evidence and the four requirements.
