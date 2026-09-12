#!/usr/bin/env bash
#
# Confirms the Kafka stack is genuinely usable, not merely running.
#
#   ./scripts/verify-stack.sh
#
# A healthy broker only means it is accepting connections. It says nothing about whether the
# topics this application needs actually exist: kafka-init is a separate one-shot job that can
# fail on its own, and auto-creation is deliberately disabled in the compose file. Without this
# check the stack reports ready while the producer cannot use it - a false green in the one
# step the live demonstration depends on, which is worse than an honest failure.

set -uo pipefail

REQUIRED_TOPICS=(orders.v1 orders.v1-retry-0 orders.v1-retry-1 orders.v1-dlt)
SCHEMA_REGISTRY_URL="http://localhost:8081"

fail() {
    echo ""
    echo "  FAILED: $1" >&2
    exit 1
}

echo "Verifying the Kafka stack..."
echo ""

# --- 1. Broker reachable -----------------------------------------------------
if ! docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "  The broker is not answering on localhost:9092." >&2
    echo "  Start the stack with:  docker compose up -d" >&2
    fail "Kafka broker unreachable."
fi
echo "  Broker           OK"

# --- 2. Wait for kafka-init, if it is still working --------------------------
# `docker compose up -d` returns as soon as the container starts, not when this one-shot job
# finishes. Checking the topic list straight away would report a false failure while topics
# are still being created.
if [ "$(docker inspect -f '{{.State.Running}}' kafka-init 2>/dev/null)" = "true" ]; then
    echo "  kafka-init       still running, waiting for it to finish..."
    docker wait kafka-init >/dev/null 2>&1
fi

# --- 3. Every required topic exists ------------------------------------------
existing=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | tr -d '\r')

missing=()
for topic in "${REQUIRED_TOPICS[@]}"; do
    if ! grep -qx "$topic" <<<"$existing"; then
        missing+=("$topic")
    fi
done

if [ ${#missing[@]} -gt 0 ]; then
    echo ""
    echo "  The broker is healthy, but these required topics are missing:" >&2
    printf '      %s\n' "${missing[@]}" >&2
    echo "" >&2
    echo "  Topic creation runs in the kafka-init container. Its last output was:" >&2
    docker logs kafka-init 2>&1 | tail -20 >&2
    echo "" >&2
    echo "  Retry topic creation with:" >&2
    echo "      docker compose up -d --force-recreate kafka-init && docker wait kafka-init" >&2
    fail "Topic creation failed; the stack is not usable."
fi
echo "  Topics           OK  (${#REQUIRED_TOPICS[@]} required topics present)"

# --- 4. Schema Registry responding -------------------------------------------
if ! curl -fsS "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
    echo "  The Schema Registry is not answering on ${SCHEMA_REGISTRY_URL}." >&2
    docker logs schema-registry 2>&1 | tail -20 >&2
    fail "Schema Registry unreachable."
fi
subjects=$(curl -fsS "${SCHEMA_REGISTRY_URL}/subjects")
echo "  Schema Registry  OK  (subjects: ${subjects})"

echo ""
echo "  Topics:"
while IFS= read -r topic; do
    [ -z "$topic" ] && continue
    for required in "${REQUIRED_TOPICS[@]}"; do
        if [ "$topic" = "$required" ]; then
            echo "      $topic"
            continue 2
        fi
    done
    echo "      $topic  (not required by this application)"
done <<<"$existing"

echo ""
echo "  Stack ready.  Kafka UI: http://localhost:8090   Schema Registry: ${SCHEMA_REGISTRY_URL}"
