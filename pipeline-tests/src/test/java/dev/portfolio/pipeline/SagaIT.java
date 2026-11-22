package dev.portfolio.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Black-box test: three real executable JARs, Kafka, and three independent databases. */
@Testcontainers
class SagaIT {
    @Container static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");
    @Container static final PostgreSQLContainer<?> ORDER_DB = database("orders");
    @Container static final PostgreSQLContainer<?> INVENTORY_DB = database("inventory");
    @Container static final PostgreSQLContainer<?> PAYMENT_DB = database("payments");
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final List<RunningService> SERVICES = new ArrayList<>();
    private static URI orderUrl;

    private static PostgreSQLContainer<?> database(String name) {
        return new PostgreSQLContainer<>("postgres:17.9-alpine").withDatabaseName(name);
    }

    @BeforeAll
    static void startServices() throws Exception {
        SERVICES.add(start("order", ORDER_DB));
        SERVICES.add(start("inventory", INVENTORY_DB));
        SERVICES.add(start("payment", PAYMENT_DB));
        for (var service : SERVICES) {
            URI base = awaitReady(service);
            if (service.name().equals("order")) {
                orderUrl = base;
            }
        }
    }

    private static URI awaitReady(RunningService service) {
        var result = new java.util.concurrent.atomic.AtomicReference<URI>();
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofMillis(300))
            .ignoreExceptionsMatching(ex -> ex instanceof java.io.IOException).untilAsserted(() -> {
            assertThat(service.process().isAlive()).withFailMessage(() -> readLog(service)).isTrue();
            var matcher = Pattern.compile("Tomcat started on port (\\d+)").matcher(readLog(service));
            assertThat(matcher.find()).withFailMessage(() -> readLog(service)).isTrue();
            var base = URI.create("http://localhost:" + matcher.group(1));
            assertThat(get(base.resolve("/actuator/health/readiness")).statusCode()).isEqualTo(200);
            result.set(base);
        });
        return result.get();
    }

    private static RunningService start(String name, PostgreSQLContainer<?> database) throws Exception {
        return start(name, database, "0");
    }

    private static RunningService start(String name, PostgreSQLContainer<?> database, String failureRate)
            throws Exception {
        return start(name, database, failureRate, true);
    }

    private static RunningService start(String name, PostgreSQLContainer<?> database, String failureRate,
                                        boolean publisherEnabled) throws Exception {
        var root = Path.of(System.getProperty("projectRoot"));
        var jar = root.resolve(name + "-service/target/" + name + "-service-"
            + System.getProperty("projectVersion") + ".jar");
        assertThat(jar).isRegularFile();
        var logs = root.resolve("pipeline-tests/target/service-logs");
        Files.createDirectories(logs);
        var log = logs.resolve(name + "-" + failureRate + "-publisher-" + publisherEnabled + ".log");
        var builder = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx192m", "-XX:ActiveProcessorCount=2", "-jar", jar.toString(),
            "--server.port=0", "--spring.datasource.url=" + database.getJdbcUrl(),
            "--spring.datasource.username=" + database.getUsername(),
            "--spring.datasource.password=" + database.getPassword(),
            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
            "--logging.level.root=INFO", "--spring.output.ansi.enabled=never")
            .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("PAYMENT_FAILURE_RATE", failureRate);
        builder.environment().put("OUTBOX_PUBLISHER_ENABLED", Boolean.toString(publisherEnabled));
        return new RunningService(name, builder.start(), log);
    }

    @AfterAll
    static void stopServices() throws Exception {
        for (var service : SERVICES) {
            service.process().destroy();
        }
        for (var service : SERVICES) {
            if (!service.process().waitFor(10, TimeUnit.SECONDS)) {
                service.process().destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            }
        }
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = "portfolio-trace-123")
    void completesSagaWithPersistedEffectsAndFourCorrelatedEvents(String requestedTrace) throws Exception {
        long stockBefore = count(INVENTORY_DB, "SELECT available FROM inventory_service.stock WHERE sku = 'DEMO-SKU'");
        var request = HttpRequest.newBuilder(orderUrl.resolve("/orders"))
            .header("Content-Type", "application/json")
            .timeout(Duration.ofSeconds(20))
            .POST(HttpRequest.BodyPublishers.ofString(
                "{\"sku\":\"DEMO-SKU\",\"quantity\":2,\"totalAmount\":19.98}"));
        if (requestedTrace != null) {
            request.header("X-Correlation-ID", requestedTrace);
        }
        var response = HTTP.send(request.build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(201);
        var created = JSON.readTree(response.body());
        String id = created.path("id").asText();
        String trace = created.path("correlationId").asText();
        assertThat(UUID.fromString(id)).isNotNull();
        assertThat(trace).isNotBlank();
        if (requestedTrace != null) {
            assertThat(trace).isEqualTo(requestedTrace);
        }
        assertThat(response.headers().allValues("X-Correlation-ID")).containsExactly(trace);
        assertThat(response.headers().firstValue("Location")).contains("/orders/" + id);
        assertThat(created.path("status").asText()).isEqualTo("CREATED");
        assertThat(created.path("currency").asText()).isEqualTo("USD");

        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            var result = get(orderUrl.resolve("/orders/" + id));
            assertThat(result.statusCode()).isEqualTo(200);
            assertThat(result.headers().allValues("X-Correlation-ID")).containsExactly(trace);
            assertThat(JSON.readTree(result.body()).path("status").asText()).isEqualTo("CONFIRMED");
        });

        var observed = readEvents(id, trace,
            "OrderCreated", "InventoryReserved", "PaymentSucceeded", "OrderConfirmed");
        for (var event : observed) {
            assertThat(UUID.fromString(event.path("eventId").asText())).isNotNull();
            assertThat(java.time.Instant.parse(event.path("occurredAt").asText())).isNotNull();
            assertThat(event.path("version").asInt()).isEqualTo(1);
            assertThat(event.path("correlationId").asText()).isEqualTo(trace);
            assertThat(event.path("payload").path("orderId").asText()).isEqualTo(id);
            assertThat(event.path("payload").path("quantity").asInt()).isEqualTo(2);
            assertThat(event.path("payload").path("sku").asText()).isEqualTo("DEMO-SKU");
            assertThat(event.path("payload").path("totalAmount").decimalValue()).isEqualByComparingTo("19.98");
        }
        assertThat(count(INVENTORY_DB, "SELECT available FROM inventory_service.stock WHERE sku = 'DEMO-SKU'"))
            .isEqualTo(stockBefore - 2);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_service.reservations WHERE order_id = '"
            + id + "' AND quantity = 2 AND sku = 'DEMO-SKU'")).isEqualTo(1);
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id = '"
            + id + "' AND total_amount = 19.98 AND status = 'SUCCEEDED'")).isEqualTo(1);
        assertThat(metric(orderUrl, "pipeline_orders_total", "outcome=\"created\"")).isEqualTo(count(ORDER_DB, "SELECT count(*) FROM order_service.orders"));
        assertThat(metric(orderUrl, "pipeline_orders_total", "outcome=\"confirmed\"")).isEqualTo(count(ORDER_DB, "SELECT count(*) FROM order_service.orders WHERE status = 'CONFIRMED'"));
        assertThat(metric(orderUrl, "pipeline_orders_total", "outcome=\"cancelled\"")).isEqualTo(count(ORDER_DB, "SELECT count(*) FROM order_service.orders WHERE status = 'CANCELLED'"));
        for (var running : SERVICES) {
            URI base = awaitReady(running);
            assertThat(metric(base, "spring_kafka_listener_seconds_count")).isGreaterThan(0);
            boolean correlated = false;
            for (String line : readLog(running).split("\\R")) {
                if (!line.startsWith("{")) continue;
                var log = JSON.readTree(line);
                if (trace.equals(log.path("correlationId").asText()) && log.path("message").asText().startsWith("Handled ")) {
                    assertThat(log.path("service").asText()).isEqualTo(running.name() + "-service");
                    correlated = true;
                }
            }
            assertThat(correlated).as("Correlated JSON consumer log from " + running.name()).isTrue();
        }

    }

    @ParameterizedTest
    @ValueSource(strings = {
        "{}",
        "{\"sku\":\"\",\"quantity\":1,\"totalAmount\":10}",
        "{\"sku\":\"DEMO-SKU\",\"quantity\":0,\"totalAmount\":10}",
        "{\"sku\":\"DEMO-SKU\",\"quantity\":1001,\"totalAmount\":10}",
        "{\"sku\":\"DEMO-SKU\",\"quantity\":1,\"totalAmount\":-1}",
        "{\"sku\":\"DEMO-SKU\",\"quantity\":1,\"totalAmount\":1.001}",
        "not-json"
    })
    void rejectsInvalidRequestsWithoutCreatingOrders(String body) throws Exception {
        long before = count(ORDER_DB, "SELECT count(*) FROM order_service.orders");
        var response = HTTP.send(HttpRequest.newBuilder(orderUrl.resolve("/orders"))
            .header("Content-Type", "application/json").timeout(Duration.ofSeconds(5))
            .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().firstValue("X-Correlation-ID")).isPresent();
        assertThat(JSON.readTree(response.body()).path("status").asInt()).isEqualTo(400);
        assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.orders")).isEqualTo(before);
    }

    @Test
    void returnsProblemDetailsForMissingAndMalformedIds() throws Exception {
        var missing = get(orderUrl.resolve("/orders/" + UUID.randomUUID()));
        assertThat(missing.statusCode()).isEqualTo(404);
        assertThat(JSON.readTree(missing.body()).path("status").asInt()).isEqualTo(404);
        assertThat(get(orderUrl.resolve("/orders/not-a-uuid")).statusCode()).isEqualTo(400);
    }

    @Test
    void rejectsUnsafeCorrelationHeader() throws Exception {
        long before = count(ORDER_DB, "SELECT count(*) FROM order_service.orders");
        var response = HTTP.send(HttpRequest.newBuilder(orderUrl.resolve("/orders"))
            .header("Content-Type", "application/json").header("X-Correlation-ID", "not a valid trace")
            .timeout(Duration.ofSeconds(5)).POST(HttpRequest.BodyPublishers.ofString(
                "{\"sku\":\"DEMO-SKU\",\"quantity\":1,\"totalAmount\":10}"))
            .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.orders")).isEqualTo(before);
    }

    @ParameterizedTest
    @CsvSource({"DEMO-SKU,1000", "UNKNOWN-SKU,1"})
    void inventoryFailureCancelsWithoutReservationOrPayment(String sku, int quantity) throws Exception {
        long stockBefore = availableStock();
        String trace = "inventory-failure-" + UUID.randomUUID();
        String id = createOrder(sku, quantity, trace);
        awaitStatus(id, "CANCELLED");
        var failureEvents = readEvents(id, trace, "OrderCreated", "InventoryFailed", "OrderCancelled");
        duplicateAndDrain(failureEvents);
        assertOutboxCounts(id, 2, 1, 0);
        assertThat(availableStock()).isEqualTo(stockBefore);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_service.reservations WHERE order_id = '"
            + id + "'")).isZero();
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id = '"
            + id + "'")).isZero();
    }

    @Test
    void paymentFailureReleasesRecordedStockBeforeCancellingAndStockCanBeReused() throws Exception {
        long stockBefore = availableStock();
        String trace = "payment-failure-" + UUID.randomUUID();
        stopPayment();
        List<JsonNode> failedEvents = new ArrayList<>();
        try {
            String id = createOrder("DEMO-SKU", 2, trace);
            // Hold payment offline so we can prove the stock was actually reserved before compensation.
            await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                assertThat(count(INVENTORY_DB,
                    "SELECT count(*) FROM inventory_service.reservations WHERE order_id = '" + id
                        + "' AND quantity = 2 AND NOT released")).isEqualTo(1);
                assertThat(availableStock()).isEqualTo(stockBefore - 2);
            });
            assertThat(JSON.readTree(get(orderUrl.resolve("/orders/" + id)).body())
                .path("status").asText()).isEqualTo("CREATED");
            startPayment("1");
            awaitStatus(id, "CANCELLED");
            failedEvents = readEvents(id, trace, "OrderCreated", "InventoryReserved", "PaymentFailed",
                "InventoryReleased", "OrderCancelled");
            assertThat(availableStock()).isEqualTo(stockBefore);
            assertThat(count(INVENTORY_DB,
                "SELECT count(*) FROM inventory_service.reservations WHERE order_id = '" + id
                    + "' AND quantity = 2 AND released")).isEqualTo(1);
            assertThat(count(PAYMENT_DB,
                "SELECT count(*) FROM payment_service.payments WHERE order_id = '" + id
                    + "' AND total_amount = 19.98 AND status = 'FAILED'")).isEqualTo(1);
        } finally {
            stopPayment();
            startPayment("0");
        }
        // Redeliver after changing the payment failure rate: an already processed decision must not change.
        duplicateAndDrain(failedEvents);
        String failedId = failedEvents.getFirst().path("payload").path("orderId").asText();
        assertThat(availableStock()).isEqualTo(stockBefore);
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id = '"
            + failedId + "' AND status = 'FAILED'")).isEqualTo(1);
        assertOutboxCounts(failedId, 2, 2, 1);
        String recoveryTrace = "after-compensation-" + UUID.randomUUID();
        String recoveryId = createOrder("DEMO-SKU", 2, recoveryTrace);
        awaitStatus(recoveryId, "CONFIRMED");
        readEvents(recoveryId, recoveryTrace,
            "OrderCreated", "InventoryReserved", "PaymentSucceeded", "OrderConfirmed");
        assertThat(availableStock()).isEqualTo(stockBefore - 2);
    }

    private static void stopPayment() throws Exception { stopService("payment", false); }

    private static void startPayment(String failureRate) throws Exception {
        restartService("payment", failureRate, true);
    }

    private static void stopService(String name, boolean crash) throws Exception {
        for (var service : List.copyOf(SERVICES)) {
            if (service.name().equals(name)) {
                if (crash) {
                    service.process().destroyForcibly();
                } else {
                    service.process().destroy();
                }
                if (!service.process().waitFor(10, TimeUnit.SECONDS)) {
                    service.process().destroyForcibly().waitFor(10, TimeUnit.SECONDS);
                }
                assertThat(service.process().isAlive()).isFalse();
                SERVICES.remove(service);
            }
        }
    }

    private static void restartService(String name, String failureRate, boolean publisherEnabled) throws Exception {
        var service = start(name, serviceDatabase(name), failureRate, publisherEnabled);
        SERVICES.add(service);
        var base = awaitReady(service);
        if (name.equals("order")) {
            orderUrl = base;
        }
    }

    private static PostgreSQLContainer<?> serviceDatabase(String name) {
        return switch (name) {
            case "order" -> ORDER_DB;
            case "inventory" -> INVENTORY_DB;
            case "payment" -> PAYMENT_DB;
            default -> throw new IllegalArgumentException(name);
        };
    }

    @Test
    void duplicateKafkaDeliveriesDoNotRepeatBusinessEffectsOrCreateNewEvents() throws Exception {
        long before = availableStock();
        String trace = "duplicates-" + UUID.randomUUID();
        String id = createOrder("DEMO-SKU", 2, trace);
        awaitStatus(id, "CONFIRMED");
        var events = readEvents(id, trace,
            "OrderCreated", "InventoryReserved", "PaymentSucceeded", "OrderConfirmed");
        duplicateAndDrain(events);
        assertThat(availableStock()).isEqualTo(before - 2);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_service.reservations WHERE order_id = '"
            + id + "' AND quantity = 2 AND NOT released")).isEqualTo(1);
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id = '"
            + id + "' AND status = 'SUCCEEDED'")).isEqualTo(1);
        assertOutboxCounts(id, 2, 1, 1);
        for (var pair : Map.of("inventory", events.get(0), "payment", events.get(1), "order", events.get(2)).entrySet()) {
            assertThat(count(serviceDatabase(pair.getKey()), "SELECT count(*) FROM " + pair.getKey()
                + "_service.processed_events WHERE event_id = '" + pair.getValue().path("eventId").asText() + "'"))
                .isEqualTo(1);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"order", "inventory", "payment"})
    void committedOutboxSurvivesProcessDeathBeforePublication(String name) throws Exception {
        long before = availableStock();
        var db = serviceDatabase(name);
        String trace = "crash-" + name + "-" + UUID.randomUUID();
        String id;
        String pendingId;
        stopService(name, false);
        try {
            // Disable only the poller. All business transactions and consumers run normally.
            restartService(name, "0", false);
            id = createOrder("DEMO-SKU", 2, trace);
            String query = "SELECT count(*) FROM " + name + "_service.outbox_events WHERE aggregate_id = '"
                + id + "' AND sent_at IS NULL";
            await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> assertThat(count(db, query)).isEqualTo(1));
            pendingId = scalar(db, "SELECT event_id FROM " + name
                + "_service.outbox_events WHERE aggregate_id = '" + id + "' AND sent_at IS NULL");
            assertThat(JSON.readTree(get(orderUrl.resolve("/orders/" + id)).body()).path("status").asText())
                .isEqualTo("CREATED");
            if (name.equals("order")) {
                assertThat(availableStock()).isEqualTo(before);
            } else {
                assertThat(availableStock()).isEqualTo(before - 2);
                assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_service.reservations WHERE order_id = '"
                    + id + "'")).isEqualTo(1);
            }
            if (name.equals("payment")) {
                assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id = '"
                    + id + "' AND status = 'SUCCEEDED'")).isEqualTo(1);
            }
            stopService(name, true); // actual forcible process termination, not an exception/mock
            assertThat(count(db, query)).isEqualTo(1);
        } finally {
            stopService(name, false);
            restartService(name, "0", true);
        }
        awaitStatus(id, "CONFIRMED");
        var events = readEvents(id, trace,
            "OrderCreated", "InventoryReserved", "PaymentSucceeded", "OrderConfirmed");
        assertThat(events).extracting(e -> e.path("eventId").asText()).contains(pendingId);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(count(db, "SELECT count(*) FROM " + name
                + "_service.outbox_events WHERE aggregate_id = '" + id + "' AND sent_at IS NULL")).isZero());
        assertThat(availableStock()).isEqualTo(before - 2);
        assertOutboxCounts(id, 2, 1, 1);
    }

    @Test
    void kafkaAcknowledgementBeforeSentMarkerFailureCausesSafeRedelivery() throws Exception {
        long before = availableStock();
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.outbox_events WHERE sent_at IS NULL")).isZero());
        // A real PostgreSQL failure after Kafka acknowledges the record reproduces the ambiguous-send window.
        execute(ORDER_DB, "ALTER TABLE order_service.outbox_events ADD CONSTRAINT test_block_sent "
            + "CHECK (sent_at IS NULL) NOT VALID");
        String id;
        List<JsonNode> deliveries = new ArrayList<>();
        try {
            id = createOrder("DEMO-SKU", 2, "ack-window-" + UUID.randomUUID());
            awaitStatus(id, "CONFIRMED");
            try (var consumer = new KafkaConsumer<String, String>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "ack-window-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
                consumer.subscribe(List.of("order-events"));
                await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                    for (var record : consumer.poll(Duration.ofMillis(200))) {
                        if (id.equals(record.key())) {
                            var event = JSON.readTree(record.value());
                            if ("OrderCreated".equals(event.path("eventType").asText())) {
                                deliveries.add(event);
                            }
                        }
                    }
                    assertThat(deliveries.size()).isGreaterThanOrEqualTo(2);
                });
            }
            assertThat(deliveries).allMatch(event -> event.equals(deliveries.getFirst()));
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.outbox_events WHERE aggregate_id = '"
                + id + "' AND sent_at IS NULL")).isEqualTo(2);
        } finally {
            execute(ORDER_DB, "ALTER TABLE order_service.outbox_events DROP CONSTRAINT test_block_sent");
        }
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.outbox_events WHERE aggregate_id = '"
                + id + "' AND sent_at IS NULL")).isZero());
        duplicateAndDrain(deliveries);
        assertThat(availableStock()).isEqualTo(before - 2);
        assertOutboxCounts(id, 2, 1, 1);
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id = '"
            + id + "' AND status = 'SUCCEEDED'")).isEqualTo(1);
    }

    @ParameterizedTest
    @ValueSource(strings = {"order", "inventory", "payment"})
    void retriesFourAttemptsThenDeadLettersAndReplaysOnlyFailedService(String name) throws Exception {
        long stockBefore = count(INVENTORY_DB, "SELECT available FROM inventory_service.stock WHERE sku = 'DEMO-SKU'");
        var db = serviceDatabase(name);
        String schema = name + "_service";
        String table = schema + switch (name) {
            case "order" -> ".orders";
            case "inventory" -> ".reservations";
            default -> ".payments";
        };
        String operation = name.equals("order") ? "UPDATE" : "INSERT";
        execute(db, "CREATE SEQUENCE " + schema + ".test_attempts");
        for (int n = 1; n <= 4; n++) execute(db, "CREATE SEQUENCE " + schema + ".test_time_" + n);
        execute(db, "CREATE FUNCTION " + schema + ".test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ "
            + "DECLARE n bigint; BEGIN n := nextval('" + schema + ".test_attempts'); "
            + "PERFORM setval('" + schema + ".test_time_' || n, (extract(epoch FROM clock_timestamp()) * 1000)::bigint); "
            + "RAISE EXCEPTION 'phase5 outage'; END $$");
        execute(db, "CREATE TRIGGER test_outage BEFORE " + operation + " ON " + table
            + " FOR EACH ROW EXECUTE FUNCTION " + schema + ".test_fail()");
        String id;
        long deadLetterId;
        String original;
        long started = System.nanoTime();
        try {
            id = createOrder("DEMO-SKU", 1, "dlq-" + UUID.randomUUID());
            await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                assertThat(count(db, "SELECT count(*) FROM " + schema
                    + ".dead_letters WHERE message_key = '" + id + "'")).isEqualTo(1));
            assertThat(Duration.ofNanos(System.nanoTime() - started).toMillis()).isGreaterThanOrEqualTo(1750);
            assertThat(count(db, "SELECT last_value FROM " + schema + ".test_attempts")).isEqualTo(4);
            // PostgreSQL sequences survive rollback, recording actual delivery times despite the injected failure.
            for (int n = 2; n <= 4; n++) {
                long previous = count(db, "SELECT last_value FROM " + schema + ".test_time_" + (n - 1));
                long current = count(db, "SELECT last_value FROM " + schema + ".test_time_" + n);
                assertThat(current - previous).isGreaterThanOrEqualTo((250L << (n - 2)) - 5);
            }
            original = scalar(db, "SELECT envelope FROM " + schema + ".dead_letters WHERE message_key = '" + id + "'");
            deadLetterId = count(db, "SELECT id FROM " + schema + ".dead_letters WHERE message_key = '" + id + "'");
            String eventId = JSON.readTree(original).path("eventId").asText();
            assertThat(count(db, "SELECT count(*) FROM " + schema + ".processed_events WHERE event_id = '" + eventId + "'"))
                .isZero();
            assertThat(JSON.readTree(get(orderUrl.resolve("/orders/" + id)).body()).path("status").asText())
                .isEqualTo("CREATED");
        } finally {
            execute(db, "DROP TRIGGER test_outage ON " + table);
            execute(db, "DROP FUNCTION " + schema + ".test_fail()");
            execute(db, "DROP SEQUENCE " + schema + ".test_attempts");
            for (int n = 1; n <= 4; n++) execute(db, "DROP SEQUENCE " + schema + ".test_time_" + n);
        }
        URI base = awaitReady(SERVICES.stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow());
        assertThat(get(base.resolve("/dead-letters")).body()).contains(Long.toString(deadLetterId));
        for (int repeat = 0; repeat < 2; repeat++) {
            var response = HTTP.send(HttpRequest.newBuilder(base.resolve("/dead-letters/" + deadLetterId + "/replay"))
                .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(202);
        }
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
            assertThat(JSON.readTree(get(orderUrl.resolve("/orders/" + id)).body()).path("status").asText())
                .isEqualTo("CONFIRMED"));
        assertThat(count(db, "SELECT count(*) FROM " + schema + ".outbox_events WHERE destination = 'order-events."
            + name + ".replay' AND message_key = '" + id + "'")).isEqualTo(1);
        assertThat(scalar(db, "SELECT envelope FROM " + schema + ".outbox_events WHERE destination = 'order-events."
            + name + ".replay' AND message_key = '" + id + "'")).isEqualTo(original);
        String eventId = JSON.readTree(original).path("eventId").asText();
        assertThat(count(db, "SELECT count(*) FROM " + schema + ".processed_events WHERE event_id = '" + eventId + "'"))
            .isEqualTo(1);
        assertOutboxCounts(id, 2, 1, 1);
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id = '" + id + "'"))
            .isEqualTo(1);
        assertThat(count(INVENTORY_DB, "SELECT available FROM inventory_service.stock WHERE sku = 'DEMO-SKU'"))
            .isEqualTo(stockBefore - 1);
        var missing = HTTP.send(HttpRequest.newBuilder(base.resolve("/dead-letters/9223372036854775807/replay"))
            .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(missing.statusCode()).isEqualTo(404);
    }

    @Test
    void malformedRecordIsQuarantinedAndFailedReplayCreatesANewDeadLetter() throws Exception {
        String key = UUID.randomUUID().toString();
        try (var producer = new KafkaProducer<String, String>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "acks", "all",
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class))) {
            producer.send(new ProducerRecord<>("order-events", key, "{invalid-json")).get(15, TimeUnit.SECONDS);
        }
        for (String name : List.of("order", "inventory", "payment")) {
            var db = serviceDatabase(name);
            await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                assertThat(count(db, "SELECT count(*) FROM " + name + "_service.dead_letters WHERE message_key = '" + key + "'"))
                    .isEqualTo(1));
        }
        long id = count(ORDER_DB, "SELECT id FROM order_service.dead_letters WHERE message_key = '" + key + "'");
        var response = HTTP.send(HttpRequest.newBuilder(orderUrl.resolve("/dead-letters/" + id + "/replay"))
            .POST(HttpRequest.BodyPublishers.noBody()).build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(202);
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.dead_letters WHERE message_key = '" + key + "'"))
                .isEqualTo(2));
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_service.dead_letters WHERE message_key = '" + key + "'"))
            .isEqualTo(1);
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.dead_letters WHERE message_key = '" + key + "'"))
            .isEqualTo(1);
    }

    @Test
    void transientDatabaseFailureRecoversWithinRetryBudgetWithoutDeadLetter() throws Exception {
        execute(PAYMENT_DB, "CREATE SEQUENCE payment_service.test_transient_attempts");
        execute(PAYMENT_DB, "CREATE FUNCTION payment_service.test_transient() RETURNS trigger LANGUAGE plpgsql AS $$ "
            + "BEGIN IF nextval('payment_service.test_transient_attempts') < 3 THEN "
            + "RAISE EXCEPTION 'temporary outage'; END IF; RETURN NEW; END $$");
        execute(PAYMENT_DB, "CREATE TRIGGER test_transient BEFORE INSERT ON payment_service.payments "
            + "FOR EACH ROW EXECUTE FUNCTION payment_service.test_transient()");
        try {
            String id = createOrder("DEMO-SKU", 1, "transient-" + UUID.randomUUID());
            await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                assertThat(JSON.readTree(get(orderUrl.resolve("/orders/" + id)).body()).path("status").asText())
                    .isEqualTo("CONFIRMED"));
            assertThat(count(PAYMENT_DB, "SELECT last_value FROM payment_service.test_transient_attempts")).isEqualTo(3);
            assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.dead_letters WHERE message_key = '" + id + "'"))
                .isZero();
            assertOutboxCounts(id, 2, 1, 1);
        } finally {
            execute(PAYMENT_DB, "DROP TRIGGER test_transient ON payment_service.payments");
            execute(PAYMENT_DB, "DROP FUNCTION payment_service.test_transient()");
            execute(PAYMENT_DB, "DROP SEQUENCE payment_service.test_transient_attempts");
        }
    }

    @Test
    void deadLetterStorageOutageKeepsRecordPendingWithoutRecursiveDlqPublication() throws Exception {
        execute(ORDER_DB, "CREATE SEQUENCE order_service.test_sink_attempts");
        execute(ORDER_DB, "CREATE FUNCTION order_service.test_sink() RETURNS trigger LANGUAGE plpgsql AS $$ "
            + "BEGIN PERFORM nextval('order_service.test_sink_attempts'); RAISE EXCEPTION 'sink outage'; END $$");
        execute(ORDER_DB, "CREATE TRIGGER test_sink BEFORE INSERT ON order_service.dead_letters "
            + "FOR EACH ROW EXECUTE FUNCTION order_service.test_sink()");
        String key = UUID.randomUUID().toString();
        try (var producer = new KafkaProducer<String, String>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "acks", "all",
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class));
             var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var sent = producer.send(new ProducerRecord<>("order-events.order.DLT", key, "broken"))
                .get(15, TimeUnit.SECONDS);
            await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
                assertThat(count(ORDER_DB, "SELECT last_value FROM order_service.test_sink_attempts")).isGreaterThanOrEqualTo(5));
            var partition = new TopicPartition(sent.topic(), sent.partition());
            long end = admin.listOffsets(Map.of(partition, org.apache.kafka.clients.admin.OffsetSpec.latest()))
                .partitionResult(partition).get(10, TimeUnit.SECONDS).offset();
            assertThat(end).isEqualTo(sent.offset() + 1);
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.dead_letters WHERE message_key = '" + key + "'"))
                .isZero();
        } finally {
            execute(ORDER_DB, "DROP TRIGGER test_sink ON order_service.dead_letters");
            execute(ORDER_DB, "DROP FUNCTION order_service.test_sink()");
            execute(ORDER_DB, "DROP SEQUENCE order_service.test_sink_attempts");
        }
        await().atMost(Duration.ofSeconds(40)).untilAsserted(() ->
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.dead_letters WHERE message_key = '" + key + "'"))
                .isEqualTo(1));
    }

    @Test
    void thirtyConcurrentOrdersCompeteForSevenUnitsWithoutOverselling() throws Exception {
        String sku = "LIMITED-" + UUID.randomUUID().toString().toUpperCase(java.util.Locale.ROOT);
        execute(INVENTORY_DB, "INSERT INTO inventory_service.stock (sku, available) VALUES ('" + sku + "', 7)");
        var requests = new ArrayList<java.util.concurrent.CompletableFuture<HttpResponse<String>>>();
        for (int i = 0; i < 30; i++) {
            var request = HttpRequest.newBuilder(orderUrl.resolve("/orders"))
                .header("Content-Type", "application/json")
                .header("X-Correlation-ID", "concurrent-" + UUID.randomUUID())
                .timeout(Duration.ofSeconds(30))
                .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of(
                    "sku", sku, "quantity", 1, "totalAmount", 1))))
                .build();
            requests.add(HTTP.sendAsync(request, HttpResponse.BodyHandlers.ofString()));
        }
        var ids = new ArrayList<String>();
        for (var request : requests) {
            var response = request.get(35, TimeUnit.SECONDS);
            assertThat(response.statusCode()).isEqualTo(201);
            ids.add(JSON.readTree(response.body()).path("id").asText());
        }
        assertThat(ids).doesNotHaveDuplicates();
        await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.orders WHERE sku = '" + sku
                + "' AND status = 'CONFIRMED'")).isEqualTo(7);
            assertThat(count(ORDER_DB, "SELECT count(*) FROM order_service.orders WHERE sku = '" + sku
                + "' AND status = 'CANCELLED'")).isEqualTo(23);
        });
        assertThat(count(INVENTORY_DB, "SELECT available FROM inventory_service.stock WHERE sku = '" + sku + "'"))
            .isZero();
        assertThat(count(INVENTORY_DB, "SELECT version FROM inventory_service.stock WHERE sku = '" + sku + "'"))
            .isEqualTo(7);
        assertThat(count(INVENTORY_DB, "SELECT sum(quantity) FROM inventory_service.reservations WHERE sku = '" + sku + "' AND NOT released"))
            .isEqualTo(7);
        String idList = "'" + String.join("','", ids) + "'";
        assertThat(count(PAYMENT_DB, "SELECT count(*) FROM payment_service.payments WHERE order_id IN (" + idList + ") AND status = 'SUCCEEDED'"))
            .isEqualTo(7);
        assertThat(count(INVENTORY_DB, "SELECT count(*) FROM inventory_service.outbox_events WHERE aggregate_id IN (" + idList + ") AND event_type = 'InventoryFailed'"))
            .isEqualTo(23);
        for (String name : List.of("order", "inventory", "payment")) {
            assertThat(count(serviceDatabase(name), "SELECT count(*) FROM " + name + "_service.dead_letters WHERE message_key IN (" + idList + ")"))
                .isZero();
        }
        // Confirm that the real inventory consumer group has three active members, not a serial test path.
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var group = admin.describeConsumerGroups(List.of("inventory-service")).all().get(10, TimeUnit.SECONDS);
            assertThat(group.get("inventory-service").members()).hasSize(3);
        }
    }

    @Test
    void committedOffsetLagRisesDuringBlockedProcessingAndReturnsToZero() throws Exception {
        var inventory = SERVICES.stream().filter(s -> s.name().equals("inventory")).findFirst().orElseThrow();
        URI base = awaitReady(inventory);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
            assertThat(metric(base, "pipeline_consumer_lag")).isZero());
        String id;
        try (var connection = DriverManager.getConnection(INVENTORY_DB.getJdbcUrl(), INVENTORY_DB.getUsername(), INVENTORY_DB.getPassword())) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("LOCK TABLE inventory_service.stock IN ACCESS EXCLUSIVE MODE");
            }
            id = createOrder("DEMO-SKU", 1, "lag-" + UUID.randomUUID());
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(metric(base, "pipeline_consumer_lag")).isGreaterThanOrEqualTo(1);
                assertThat(metric(base, "pipeline_consumer_lag_last_success_seconds")).isGreaterThan(0);
            });
            connection.rollback();
        }
        awaitStatus(id, "CONFIRMED");
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
            assertThat(metric(base, "pipeline_consumer_lag")).isZero());
    }

    @Test
    void correlationHeadersCoverErrorsAndDoNotLeakBetweenRequests() throws Exception {
        for (var running : SERVICES) {
            URI base = awaitReady(running);
            String trace = "http-error-" + UUID.randomUUID();
            var response = HTTP.send(HttpRequest.newBuilder(base.resolve("/dead-letters?after=-1"))
                .header("X-Correlation-ID", trace).GET().build(), HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(400);
            assertThat(response.headers().firstValue("X-Correlation-ID")).contains(trace);
            var next = get(base.resolve("/dead-letters"));
            assertThat(next.statusCode()).isEqualTo(200);
            assertThat(next.headers().firstValue("X-Correlation-ID")).isPresent().get().isNotEqualTo(trace);
            assertThat(metric(base, "pipeline_dlq_pending")).isEqualTo(count(serviceDatabase(running.name()),
                "SELECT count(*) FROM " + running.name() + "_service.dead_letters WHERE replay_requested_at IS NULL"));
        }
    }

    private static double metric(URI base, String metric) throws Exception {
        return metric(base, metric, null);
    }

    private static double metric(URI base, String metric, String requiredLabel) throws Exception {
        var response = get(base.resolve("/actuator/prometheus"));
        assertThat(response.statusCode()).isEqualTo(200);
        var pattern = Pattern.compile("^" + Pattern.quote(metric) + "(?:\\{[^}]*})? ([^ \\n]+)", Pattern.MULTILINE);
        var matcher = pattern.matcher(response.body());
        double total = 0;
        boolean found = false;
        while (matcher.find()) {
            if (requiredLabel != null && !matcher.group().contains(requiredLabel)) continue;
            found = true;
            total += Double.parseDouble(matcher.group(1));
        }
        assertThat(found).as(metric + " exists in Prometheus scrape").isTrue();
        return total;
    }

    private static void execute(PostgreSQLContainer<?> db, String sql) throws Exception {
        try (var connection = DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static void assertOutboxCounts(String id, long order, long inventory, long payment) throws Exception {
        for (var entry : Map.of("order", order, "inventory", inventory, "payment", payment).entrySet()) {
            assertThat(count(serviceDatabase(entry.getKey()), "SELECT count(*) FROM " + entry.getKey()
                + "_service.outbox_events WHERE aggregate_id = '" + id + "'")).isEqualTo(entry.getValue());
        }
    }

    private static void duplicateAndDrain(List<JsonNode> events) throws Exception {
        var offsets = new java.util.HashMap<TopicPartition, Long>();
        try (var producer = new KafkaProducer<String, String>(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "acks", "all",
                "key.serializer", StringSerializer.class, "value.serializer", StringSerializer.class))) {
            for (int copy = 0; copy < 3; copy++) {
                for (var event : events) {
                    var metadata = producer.send(new ProducerRecord<>("order-events",
                        event.path("payload").path("orderId").asText(), JSON.writeValueAsString(event)))
                        .get(15, TimeUnit.SECONDS);
                    offsets.merge(new TopicPartition(metadata.topic(), metadata.partition()), metadata.offset() + 1, Math::max);
                }
            }
        }
        // Wait for each real consumer group to commit past the duplicate records before checking DB state.
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                for (String group : List.of("order-service", "inventory-service", "payment-service")) {
                    var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                    for (var entry : offsets.entrySet()) {
                        assertThat(committed).containsKey(entry.getKey());
                        assertThat(committed.get(entry.getKey()).offset()).isGreaterThanOrEqualTo(entry.getValue());
                    }
                }
            });
        }
    }

    private static String scalar(PostgreSQLContainer<?> db, String sql) throws Exception {
        try (var connection = DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
             var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            assertThat(result.next()).isTrue();
            return result.getString(1);
        }
    }

    private static String createOrder(String sku, int quantity, String trace) throws Exception {
        var response = HTTP.send(HttpRequest.newBuilder(orderUrl.resolve("/orders"))
            .header("Content-Type", "application/json").header("X-Correlation-ID", trace)
            .timeout(Duration.ofSeconds(20))
            .POST(HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(Map.of(
                "sku", sku, "quantity", quantity, "totalAmount", new java.math.BigDecimal("19.98")))))
            .build(), HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(201);
        assertThat(response.headers().firstValue("X-Correlation-ID")).contains(trace);
        return JSON.readTree(response.body()).path("id").asText();
    }

    private static void awaitStatus(String id, String status) {
        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            var response = get(orderUrl.resolve("/orders/" + id));
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(JSON.readTree(response.body()).path("status").asText()).isEqualTo(status);
        });
    }

    private static long availableStock() throws Exception {
        return count(INVENTORY_DB, "SELECT available FROM inventory_service.stock WHERE sku = 'DEMO-SKU'");
    }

    private static List<JsonNode> readEvents(String id, String trace, String... types) throws Exception {
        var observed = new ArrayList<JsonNode>();
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "assert-events-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
            consumer.subscribe(List.of("order-events"));
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    if (id.equals(record.key())) {
                        observed.add(JSON.readTree(record.value()));
                    }
                }
                assertThat(observed).extracting(e -> e.path("eventType").asText())
                    .containsExactly(types);
            });
        }
        assertThat(observed).extracting(e -> e.path("eventId").asText()).doesNotHaveDuplicates();
        for (var event : observed) {
            assertThat(UUID.fromString(event.path("eventId").asText())).isNotNull();
            assertThat(java.time.Instant.parse(event.path("occurredAt").asText())).isNotNull();
            assertThat(event.path("version").asInt()).isEqualTo(1);
            assertThat(event.path("correlationId").asText()).isEqualTo(trace);
            assertThat(event.path("payload").path("orderId").asText()).isEqualTo(id);
        }
        return observed;
    }

    private static HttpResponse<String> get(URI uri) throws Exception {
        return HTTP.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(3)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static long count(PostgreSQLContainer<?> db, String sql) throws Exception {
        try (var connection = DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
             var statement = connection.createStatement(); var result = statement.executeQuery(sql)) {
            result.next();
            return result.getLong(1);
        }
    }

    private static String readLog(RunningService service) {
        try { return Files.readString(service.log()); }
        catch (Exception ex) { return ex.toString(); }
    }

    private record RunningService(String name, Process process, Path log) {}
}
