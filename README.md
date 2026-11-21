# Event-driven order/payment pipeline

A Java backend portfolio project built in nine reviewable phases. **Current phase: 3 — failure paths and compensation.**
Orders coordinate through Kafka: confirm successful orders, cancel inventory rejections, and release reserved stock when mock payments fail.

## Modules

- `common-events`: transport-neutral event envelope (`eventId`, `eventType`, `correlationId`, `occurredAt`, `version`, `payload`).
- `order-service`: create/get order API and order status owner. Event timeline comes later.
- `inventory-service`: persistent stock, reservations, and compensating releases.
- `payment-service`: persistent mock payment outcomes with configurable failure injection.
- `pipeline-tests`: test-only module that launches all three executable JARs with Kafka and isolated databases.

Java 21, Spring Boot 3.5.14, Maven, Spring Kafka, PostgreSQL 17, and Flyway.

## Run locally

Install Docker Engine with Compose v2. From the repository root:

```sh
docker compose up --build -d
```

The image build compiles the services with Java 21. Initial startup downloads images and dependencies.
Compose waits for PostgreSQL and Kafka health checks before starting the services.
When using Podman Compose to upgrade existing containers after code changes, run `docker compose up --build --force-recreate -d` to ensure they use the rebuilt images. Named database volumes are retained.

| Service | Health | Host database port |
| --- | --- | --- |
| Order | http://localhost:8081/actuator/health | 5433 |
| Inventory | http://localhost:8082/actuator/health | 5434 |
| Payment | http://localhost:8083/actuator/health | 5435 |

Each also exposes `/actuator/health/liveness` and `/actuator/health/readiness`.
Readiness includes database connectivity. Kafka connectivity and the consumer pipeline are checked by integration tests; readiness does not yet detect a broker outage.
Kafka is reachable at `localhost:9092` from the host and `kafka:19092` inside Compose.

```sh
docker compose ps
docker compose logs -f order-service
docker compose down
```

Named volumes preserve databases and Kafka data. `docker compose down -v` deliberately deletes all local data.
Ports bind only to loopback. Credentials in Compose are local demo defaults, not production secrets.

## Demo the happy path

```sh
curl -i -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -H 'X-Correlation-ID: portfolio-demo-1' \
  -d '{"sku":"DEMO-SKU","quantity":2,"totalAmount":19.98}'
```

The API returns `201 Created`, a `Location: /orders/<id>` header, and a `CREATED` order snapshot. Omit `X-Correlation-ID` to generate one. Poll the returned location:

```sh
curl http://localhost:8081/orders/<id>
```

It becomes `CONFIRMED` after `OrderCreated → InventoryReserved → PaymentSucceeded → OrderConfirmed`.
The flow stores a reservation and a successful mock charge in their owning databases. Valid requests require an uppercase SKU (letters, digits, `_`, `-`; max 64 characters), quantity 1–1000, and a positive total with at most two decimal places and 12 integer digits. Invalid JSON/input returns a 400 problem response; unknown order IDs return 404.

Demo stock is seeded once by Flyway, not reset on restart. Repeated orders consume stock; intentionally resetting all Compose volumes also removes all orders and Kafka history.

## Demo the failure paths

An unknown SKU (for example `UNKNOWN-SKU`) or a quantity exceeding available stock emits `InventoryFailed`, then `OrderCancelled`. No reservation or payment is created. For the seeded catalog, this request exceeds available stock:

```sh
curl -i -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"sku":"DEMO-SKU","quantity":1000,"totalAmount":19.98}'
```

To force mock payments to fail, recreate only payment-service with a failure rate of 1:

```sh
PAYMENT_FAILURE_RATE=1 docker compose up -d --no-deps --force-recreate payment-service
```

Wait for payment-service to be healthy, then submit the normal happy-path request above and poll its order URL. The sequence is:

```text
OrderCreated → InventoryReserved → PaymentFailed → InventoryReleased → OrderCancelled
```

`CANCELLED` means the stock release committed. Until `InventoryReleased` arrives, the order remains `CREATED`; a separate pending-compensation status is deliberately deferred. The reservation is retained with `released=true` for inspection, and the payment record has status `FAILED`. Release uses the reservation's stored SKU and quantity.

Reset payments to successful mode:

```sh
PAYMENT_FAILURE_RATE=0 docker compose up -d --no-deps --force-recreate payment-service
```

`PAYMENT_FAILURE_RATE` defaults to `0`, accepts probabilities from `0` through `1`, and is validated at startup. For example, `0.25` fails about 25% of attempts. `0` and `1` are deterministic for demos/tests. Changes take effect on service recreation; the runtime dashboard toggle is planned for Phase 8. This variable can also be set in a local `.env` file (ignored by Git).

## Build and test

Use JDK 21 and Maven 3.6.3+ with a running Docker-compatible runtime:

```sh
mvn -B -ntp verify
```

`verify` runs unit tests and Failsafe integration tests (`*IT`) against real PostgreSQL and Kafka containers.
Tests fail if the container runtime is unavailable; they are not silently skipped.
Each service test verifies Flyway startup, all three health endpoints, and broker connectivity.
`pipeline-tests` launches the actual packaged services as separate Java processes and verifies the happy-path sequence, persisted stock/reservation/payment effects, supplied/generated correlation IDs, validation, and 404 responses. It also tests unknown SKUs, shortages, and forced payment failures: it proves stock was reserved before payment, verifies all five compensation events, checks restored stock and terminal cancellation, then successfully reuses the stock. Payment failure rates are supplied through the actual environment variable, with each test restoring the default mode. Run the full reactor from the repository root so the service JARs are built first. Child processes stop after the suite; their diagnostic logs are in `pipeline-tests/target/service-logs/`.
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

- Separate PostgreSQL containers isolate service ownership at a small local resource cost. V1 creates each service namespace; V2 creates its business tables; V3 adds cancellation, failed payments, and release tracking without rewriting earlier migrations.
- One Kafka broker/controller in KRaft mode keeps local startup simple. It provides no broker redundancy, uses plaintext locally, and needs no Zookeeper.
- A single parameterized Dockerfile builds the small reactor in a shared layer, then selects each service JAR. This keeps build logic and dependency downloads reusable across images; host Java is unnecessary for Compose startup.
- The shared module contains contracts only. Business behavior stays in its owning service.
- Schema changes belong to Flyway; Hibernate is configured to validate, never create or update tables.
- One SKU per order, a fixed USD currency, and a seeded `DEMO-SKU` with 100 units keep the example small. The caller supplies the total mock charge; this is not a pricing or real payment system.
- All event types use the `order-events` topic with three partitions, keyed by order UUID. Each service has a separate consumer group and ignores unrelated event types. A shared order payload keeps this small contract explicit.
- Business transactions commit before synchronous Kafka publication. **There is currently a crash window between commit and publish, and duplicate delivery is not safe.** Phase 4 adds an outbox and processed-event records. A publication timeout can leave an order saved with an uncertain event outcome; the API reports 503 with its order ID. Do not blindly retry a POST.
- Inventory shortages and mock payment declines are business outcomes, so they emit failure events rather than throwing infrastructure errors. Database and broker faults still surface as technical errors. Configured retries/DLQ arrive in Phase 5 and concurrency safety in Phase 6.
- On a payment failure, inventory emits `InventoryReleased` after its release transaction, and order-service then records cancellation. Release marks the existing reservation and restores stock in one database transaction. Its released flag prevents sequential repeated releases from adding stock again, but it does not replace the processed-event table or concurrency controls still planned in Phases 4 and 6. Terminal order states cannot change from confirmed to cancelled or vice versa.
- Correlation IDs are carried in the envelope and successful order HTTP responses. Listener processing uses MDC; full HTTP MDC coverage, error-response correlation, and structured logging remain later observability work.

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
