# Stack commands

Every command runs from the repository root. Docker Desktop must be running first.

## Start

```bash
docker compose up -d
```

Brings up the broker, the Schema Registry and Kafka UI, and runs the one-shot `kafka-init`
job that creates all four topics. Auto-creation is disabled, so these topics exist only
because `kafka-init` created them.

## Verify

```bash
./scripts/verify-stack.sh          # bash / Git Bash
.\scripts\verify-stack.ps1         # PowerShell
```

Checks the broker answers, all four required topics exist, and the Schema Registry responds.
Exits non-zero if anything is missing. A healthy broker on its own does not mean the stack is
usable, which is why this step exists.

## Endpoints

| Service | URL |
| --- | --- |
| Kafka UI | http://localhost:8090 |
| Schema Registry | http://localhost:8081 |
| Kafka broker (from the host) | `localhost:9092` |

## Inspect

```bash
docker compose ps                                                    # service health
docker logs kafka-init                                               # topic creation output
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic orders.v1
curl -s http://localhost:8081/subjects                               # registered Avro schemas
```

Read the messages on a topic, Avro included, decoded by the console consumer:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic orders.v1 --from-beginning --max-messages 10
```

Consumer group position and lag:

```bash
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group orders-consumer-v1
```

## Recover from a failed topic creation

```bash
docker compose up -d --force-recreate kafka-init && docker wait kafka-init
```

`docker wait` matters: `up -d` returns as soon as the container starts, not when the one-shot
job finishes, so checking the topic list immediately would report a false failure.

## Reset

```bash
docker compose down          # stop, keep the Kafka data volume
docker compose down -v       # stop and wipe all topic data for a clean demonstration run
docker compose up -d         # topics are recreated automatically on the way back up
```

Use `down -v` before recording, so the running average starts from zero and old demonstration
orders do not appear in the topics.

## Stop

```bash
docker compose down
```
