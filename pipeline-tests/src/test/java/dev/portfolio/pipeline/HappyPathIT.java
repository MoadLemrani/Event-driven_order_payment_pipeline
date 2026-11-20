package dev.portfolio.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
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
class HappyPathIT {
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
            await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofMillis(300)).untilAsserted(() -> {
                assertThat(service.process().isAlive()).withFailMessage(() -> readLog(service)).isTrue();
                var matcher = Pattern.compile("Tomcat started on port (\\d+)").matcher(readLog(service));
                assertThat(matcher.find()).withFailMessage(() -> readLog(service)).isTrue();
                var base = URI.create("http://localhost:" + matcher.group(1));
                assertThat(get(base.resolve("/actuator/health/readiness")).statusCode()).isEqualTo(200);
                if (service.name().equals("order")) {
                    orderUrl = base;
                }
            });
        }
    }

    private static RunningService start(String name, PostgreSQLContainer<?> database) throws Exception {
        var root = Path.of(System.getProperty("projectRoot"));
        var jar = root.resolve(name + "-service/target/" + name + "-service-"
            + System.getProperty("projectVersion") + ".jar");
        assertThat(jar).isRegularFile();
        var logs = root.resolve("pipeline-tests/target/service-logs");
        Files.createDirectories(logs);
        var log = logs.resolve(name + ".log");
        var process = new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-Xmx192m", "-XX:ActiveProcessorCount=2", "-jar", jar.toString(),
            "--server.port=0", "--spring.datasource.url=" + database.getJdbcUrl(),
            "--spring.datasource.username=" + database.getUsername(),
            "--spring.datasource.password=" + database.getPassword(),
            "--spring.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
            "--logging.level.root=INFO", "--spring.output.ansi.enabled=never")
            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        return new RunningService(name, process, log);
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
        assertThat(response.headers().firstValue("X-Correlation-ID")).contains(trace);
        assertThat(response.headers().firstValue("Location")).contains("/orders/" + id);
        assertThat(created.path("status").asText()).isEqualTo("CREATED");
        assertThat(created.path("currency").asText()).isEqualTo("USD");

        await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
            var result = get(orderUrl.resolve("/orders/" + id));
            assertThat(result.statusCode()).isEqualTo(200);
            assertThat(result.headers().firstValue("X-Correlation-ID")).contains(trace);
            assertThat(JSON.readTree(result.body()).path("status").asText()).isEqualTo("CONFIRMED");
        });

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
                    .containsExactly("OrderCreated", "InventoryReserved", "PaymentSucceeded", "OrderConfirmed");
            });
        }
        assertThat(observed).extracting(e -> e.path("eventId").asText()).doesNotHaveDuplicates();
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
