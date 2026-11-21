# Event-driven order/payment pipeline

A Java backend portfolio project built in nine reviewable phases. **Current phase: 5 — retries, dead letters, and durable replay.**
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

The API returns `201 Created`, a `Location: /orders/<id>` header, and a `CREATED` order snapshot after committing the order and its outgoing event to PostgreSQL. It does not wait for Kafka publication. Omit `X-Correlation-ID` to generate one. Poll the returned location:

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

## Reliability model (Phase 4)

Each service owns `outbox_events` and `processed_events` tables in its database:

1. Order creation saves the order and serialized `OrderCreated` envelope in one transaction.
2. A consumer inserts the incoming `eventId` with `ON CONFLICT DO NOTHING`. Only the transaction that claims the ID applies business changes and appends an outgoing event. A rollback undoes the claim, business changes, and outgoing event together.
3. A scheduled publisher polls at most 50 pending rows, sends their stored envelopes to Kafka, waits for broker acknowledgements, then marks them sent. The envelope, `eventId`, timestamp, correlation ID, and order key remain unchanged across retries.
4. A crash before publication leaves pending rows for restart. A failure after Kafka accepts a record but before the database commit can deliver it again; processed-event claims prevent repeated effects.

This is **at-least-once transport with idempotent processing by event ID**, not a cross-system exactly-once transaction. Claims use a database uniqueness constraint rather than an unsafe check-then-insert. They cover stock reservations/releases, mock payment decisions, and terminal order transitions. Distinct event IDs are distinct inputs; HTTP POST retries also create new orders and are not deduplicated by this mechanism.

JPA business writes and JDBC reliability-table writes share Spring's transaction and data source. A transaction-scoped PostgreSQL advisory lock permits one publisher per service database, including when more than one application instance polls. Each batch holds the database transaction while awaiting Kafka; this favors a small, understandable implementation over throughput. A failed send rolls back the batch's sent markers, and the next poll retries. A failing oldest row can delay later rows; advanced relay scaling and isolation are deferred.

`OUTBOX_PUBLISHER_ENABLED` defaults to `true` and `OUTBOX_POLL_INTERVAL_MS` to `250` for each standalone service. Compose exposes separate `ORDER_OUTBOX_PUBLISHER_ENABLED`, `INVENTORY_OUTBOX_PUBLISHER_ENABLED`, and `PAYMENT_OUTBOX_PUBLISHER_ENABLED` switches plus the shared polling interval. Disabling a publisher leaves consumers and business transactions running normally.

### Demo a crash before publication

Pause only the order outbox publisher, keeping the order API available:

```sh
ORDER_OUTBOX_PUBLISHER_ENABLED=false docker compose up -d --no-deps --force-recreate order-service
```

Wait for readiness, then submit a valid order using the earlier curl example. It remains `CREATED`. Inspect the durable pending envelope:

```sh
docker compose exec -T order-db psql -U order -d order_db -c \
  "SELECT event_id, aggregate_id, event_type, sent_at FROM order_service.outbox_events WHERE sent_at IS NULL;"
```

Kill the API process and recreate it with publishing restored (database volumes remain intact):

```sh
docker compose kill -s SIGKILL order-service
ORDER_OUTBOX_PUBLISHER_ENABLED=true docker compose up -d --no-deps --force-recreate order-service
```

Poll the saved order ID: the pending event is published and the order reaches `CONFIRMED` when payments succeed and stock is available. No second POST is needed.

## Retries, dead letters, and replay (Phase 5)

Each service retries a failed Kafka record three times after the first attempt, with exponential delays of **250 ms, 500 ms, and 1 second**. These are blocking retries: the consumer's assigned partitions wait briefly, preserving normal processing order without retry-topic infrastructure. Exception type changes do not restart the retry budget. Business declines still emit saga events; they do not enter the DLQ.

After four failed attempts, the handler publishes the raw record to `order-events.<service>.DLT` (`order`, `inventory`, or `payment`). It waits for Kafka acknowledgement before allowing the failed input offset to advance. Failed DLT publication is retried rather than silently dropping the record. A separate listener persists the raw payload, key, error message, DLT topic/partition/offset, and timestamps in that service's `dead_letters` table. The DLT locator is unique, so redelivery of the same DLT record is harmless. A database outage in this listener keeps its offset pending; it never recursively publishes to its own DLQ. The broker-to-database handoff is at least once, so a crash during original recovery can still produce multiple DLT records for one event.

All three services expose:

- `GET /dead-letters?after=0`: up to 100 records ordered by ID; use the last ID as `after` for the next page. Includes raw envelope, error, and replay-request timestamp.
- `POST /dead-letters/{id}/replay`: returns `202 Accepted`, or `404` if absent. Atomically enqueues a replay in the outbox and marks the request. Repeating this request for the same row does not enqueue again.

Replay publishes to `order-events.<service>.replay`, consumed only by the failed service. It preserves the raw payload, original event ID, correlation ID, and Kafka key. Outbox publication IDs for replay are separate from the event IDs inside the unchanged envelope. Fix the underlying database/schema/code problem first: replay does not edit malformed events or bypass deduplication. A repeated failure creates a new DLT row with its own replay action. `replayRequestedAt` means queued, not successfully processed; use the order state and subsequent dead letters to determine the outcome. Replaying a record already processed is a safe no-op at the business layer.

Example for payment-service (ports 8081 and 8082 expose the same API for order and inventory):

```sh
curl -s http://localhost:8083/dead-letters
# Replace 1 with an ID returned above, after fixing the failure:
curl -i -X POST http://localhost:8083/dead-letters/1/replay
```

The end-to-end tests demonstrate real database outages, four delivery attempts, dead-letter persistence, and recovery after repair for every service. Tests measure increasing retry delays, verify transient recovery without a DLQ entry, and prove that a DLQ storage outage cannot create a recursive publish loop. Replay enqueueing and its status marker roll back together. Malformed JSON is also quarantined, and a failed replay returns to the failed service's DLQ only. Mock payment declines are intentionally not technical failures, so the payment failure-rate switch does not populate this page.

Operational trade-offs: DLQ handoff allows later records to progress, so replay can arrive out of order. This small saga's state guards and stable event IDs are the safety boundary; arbitrary historical reordering is not supported. Dead letters and replay outbox rows are retained without automatic pruning. APIs are unauthenticated for the local portfolio demo; protect operator endpoints before external deployment. Polling publication remains an indefinitely retried durable queue, distinct from the bounded consumer retry budget. Topic replication remains one for local development.

## Build and test

Use JDK 21 and Maven 3.6.3+ with a running Docker-compatible runtime:

```sh
mvn -B -ntp verify
```

`verify` runs unit tests and Failsafe integration tests (`*IT`) against real PostgreSQL and Kafka containers.
Tests fail if the container runtime is unavailable; they are not silently skipped.
Each service test verifies Flyway startup, all three health endpoints, and broker connectivity. Forced database constraint failures also prove that business writes, processed-event claims, and outbox rows roll back together and can be retried.
`pipeline-tests` launches the actual packaged services as separate Java processes and verifies the happy-path sequence, persisted stock/reservation/payment effects, supplied/generated correlation IDs, validation, and 404 responses. It also tests unknown SKUs, shortages, and forced payment failures: it proves stock was reserved before payment, verifies all five compensation events, checks restored stock and terminal cancellation, then successfully reuses the stock. Payment failure rates are supplied through the actual environment variable, with each test restoring the default mode. Run the full reactor from the repository root so the service JARs are built first. Child processes stop after the suite; their diagnostic logs are in `pipeline-tests/target/service-logs/`.
The suite also forcibly terminates each service after a business transaction commits but before its publisher runs, then proves restart recovers the original event ID. Duplicate events traverse the actual Kafka consumers; tests wait for all consumer groups to commit past those records before checking stock, payment, and outgoing-event counts. Compensation messages are replayed after switching payments from failure to success, proving a processed decision does not change. A database constraint that rejects sent-marker updates reproduces Kafka acknowledgement followed by database failure and proves safe redelivery of the identical envelope.

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

- Separate PostgreSQL containers isolate service ownership at a small local resource cost. V1 creates each service namespace; V2 creates its business tables; V3 adds cancellation, failed payments, and release tracking; V4 adds outbox and processed-event tables; V5 adds dead-letter storage and outbox routing for replay. Earlier migrations are unchanged.
- One Kafka broker/controller in KRaft mode keeps local startup simple. It provides no broker redundancy, uses plaintext locally, and needs no Zookeeper.
- A single parameterized Dockerfile builds the small reactor in a shared layer, then selects each service JAR. This keeps build logic and dependency downloads reusable across images; host Java is unnecessary for Compose startup.
- The shared module contains contracts only. Business behavior stays in its owning service.
- Schema changes belong to Flyway; Hibernate is configured to validate, never create or update tables.
- One SKU per order, a fixed USD currency, and a seeded `DEMO-SKU` with 100 units keep the example small. The caller supplies the total mock charge; this is not a pricing or real payment system.
- All event types use the `order-events` topic with three partitions, keyed by order UUID. Each service has a separate consumer group and ignores unrelated event types. A shared order payload keeps this small contract explicit.
- Business changes, processed-event claims, and outgoing events commit atomically in each service database. Publishers provide at-least-once delivery using stable event IDs; consumers deduplicate those IDs. Sent outbox rows and processed IDs are retained for now. Deleting deduplication history while old events can still be replayed would break this guarantee.
- V4 does not backfill IDs for events processed before this phase or recover events lost before an outbox existed. Preserve existing consumer offsets during upgrades; do not replay pre-V4 history into databases containing old business effects.
- The reliability adapters remain local to each of the three services; `common-events` stays limited to contracts. A shared runtime library is deferred until further changes justify that abstraction.
- Inventory shortages and mock payment declines are business outcomes, so they emit failure events rather than throwing infrastructure errors. Database and broker faults still surface as technical errors. Technical consumer failures use bounded retries and DLQ recovery; concurrency safety arrives in Phase 6.
- On a payment failure, inventory emits `InventoryReleased` after its release transaction, and order-service then records cancellation. Release marks the existing reservation and restores stock in one database transaction. Its released flag prevents sequential repeated releases from adding stock again, and the processed-event table now prevents repeated incoming events from running the transaction again. Concurrency safety between different orders still arrives in Phase 6. Terminal order states cannot change from confirmed to cancelled or vice versa.
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
