# Event-driven order/payment pipeline

A Java backend portfolio project built in nine reviewable phases. **Current phase: 1 — skeleton.**
Order processing and saga behavior begin in Phase 2; there are no business REST endpoints yet.

## Modules

- `common-events`: transport-neutral event envelope (`eventId`, `eventType`, `correlationId`, `occurredAt`, `version`, `payload`).
- `order-service`: future order API, saga status owner, and event timeline.
- `inventory-service`: future stock reservation and compensation.
- `payment-service`: future mock payments and failure injection.

Java 21, Spring Boot 3.5.14, Maven, Spring Kafka, PostgreSQL 17, and Flyway.

## Run locally

Install Docker Engine with Compose v2. From the repository root:

```sh
docker compose up --build -d
```

The image build compiles the services with Java 21. Initial startup downloads images and dependencies.
Compose waits for PostgreSQL and Kafka health checks before starting the services.

| Service | Health | Host database port |
| --- | --- | --- |
| Order | http://localhost:8081/actuator/health | 5433 |
| Inventory | http://localhost:8082/actuator/health | 5434 |
| Payment | http://localhost:8083/actuator/health | 5435 |

Each also exposes `/actuator/health/liveness` and `/actuator/health/readiness`.
Readiness includes database connectivity. Kafka connectivity is tested separately; the skeleton has no active consumers yet.
Kafka is reachable at `localhost:9092` from the host and `kafka:19092` inside Compose.

```sh
docker compose ps
docker compose logs -f order-service
docker compose down
```

Named volumes preserve databases and Kafka data. `docker compose down -v` deliberately deletes all local data.
Ports bind only to loopback. Credentials in Compose are local demo defaults, not production secrets.

## Build and test

Use JDK 21 and Maven 3.6.3+ with a running Docker-compatible runtime:

```sh
mvn -B -ntp verify
```

`verify` runs unit tests and Failsafe integration tests (`*IT`) against real PostgreSQL and Kafka containers.
Tests fail if the container runtime is unavailable; they are not silently skipped.
Each service test verifies Flyway V1, all three health endpoints, and broker connectivity.
The shared contract has validation tests. Runtime images run as an unprivileged user.

For rootless Podman on Linux, enable its API socket and set the following before Maven:

```sh
systemctl --user start podman.socket
export DOCKER_HOST="unix://${XDG_RUNTIME_DIR}/podman/podman.sock"
export TESTCONTAINERS_RYUK_DISABLED=true
mvn -B -ntp verify
```

Ryuk is disabled only for this rootless Podman workaround; normal Docker runs use automatic cleanup.
JUnit stops test containers on normal completion; interrupted Podman runs may need manual cleanup.

## Decisions and trade-offs

- Separate PostgreSQL containers isolate service ownership at a small local resource cost. Each V1 migration creates the service namespace; business tables will be added by later migrations.
- One Kafka broker/controller in KRaft mode keeps local startup simple. It provides no broker redundancy, uses plaintext locally, and needs no Zookeeper.
- A single parameterized Dockerfile builds the small reactor in a shared layer, then selects each service JAR. This keeps build logic and dependency downloads reusable across images; host Java is unnecessary for Compose startup.
- The shared module contains contracts only. Business behavior stays in its owning service.
- Schema changes belong to Flyway; Hibernate is configured to validate, never create or update tables.
- No extra framework abstractions or empty controller/repository layers until business behavior needs them.

## Roadmap

1. Skeleton, isolated databases, health checks, container-backed smoke tests.
2. Happy-path saga and end-to-end integration test.
3. Failure paths and inventory compensation.
4. Transactional outbox and idempotent consumers with failure/duplicate tests.
5. Exponential retries, dead-letter topics, replay API.
6. Optimistic locking and concurrent stock tests.
7. Metrics, Prometheus, Grafana, structured correlated logs.
8. React/TypeScript dashboard, timeline, DLQ replay, payment failure toggle.
9. GitHub Actions and full architecture/demo/trade-off documentation.

Each phase is built, tested, and committed before the next starts.
