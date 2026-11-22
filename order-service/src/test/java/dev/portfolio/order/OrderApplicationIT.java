package dev.portfolio.order;

import org.junit.jupiter.api.Test;
import dev.portfolio.order.application.OrderService;
import dev.portfolio.events.EventEnvelope;
import dev.portfolio.events.OrderData;
import dev.portfolio.events.OrderEvents;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.apache.kafka.clients.admin.AdminClient;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;

@org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability
@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderApplicationIT {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17.9-alpine");
    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:3.9.1");

    @DynamicPropertySource
    static void configure(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("outbox.publisher-enabled", () -> false);
    }

    private final TestRestTemplate http;
    private final JdbcTemplate jdbc;
    private final OrderService service;
    private final dev.portfolio.order.messaging.DeadLetterStore deadLetters;

    @Autowired
    OrderApplicationIT(TestRestTemplate http, JdbcTemplate jdbc, OrderService service,
                       dev.portfolio.order.messaging.DeadLetterStore deadLetters) {
        this.http = http;
        this.jdbc = jdbc;
        this.service = service;
        this.deadLetters = deadLetters;
    }

    @Test
    void replayRequestAndOutboxRollBackTogetherAndDuplicateCaptureIsHarmless() {
        var record = new org.apache.kafka.clients.consumer.ConsumerRecord<String, String>(
            "order-events.order.DLT", 0, 9001L, "test-replay", "{invalid");
        deadLetters.capture(record);
        deadLetters.capture(record);
        long id = jdbc.queryForObject("SELECT id FROM order_service.dead_letters WHERE record_offset = 9001", Long.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.dead_letters WHERE record_offset = 9001", Long.class))
            .isEqualTo(1);
        assertThat(http.getForEntity("/actuator/prometheus", String.class).getBody())
            .containsPattern("(?m)^pipeline_dlq_pending\\{[^\\n]*} 1(?:\\.0)?$");
        jdbc.execute("ALTER TABLE order_service.dead_letters ADD CONSTRAINT test_replay_block CHECK (replay_requested_at IS NULL)");
        try {
            assertThatThrownBy(() -> deadLetters.replay(id)).isInstanceOf(RuntimeException.class);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.outbox_events WHERE message_key = 'test-replay'", Long.class))
                .isZero();
        } finally {
            jdbc.execute("ALTER TABLE order_service.dead_letters DROP CONSTRAINT test_replay_block");
        }
        deadLetters.replay(id);
        deadLetters.replay(id);
        assertThat(http.getForEntity("/actuator/prometheus", String.class).getBody())
            .containsPattern("(?m)^pipeline_dlq_pending\\{[^\\n]*} 0(?:\\.0)?$");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.outbox_events WHERE message_key = 'test-replay' AND sent_at IS NULL", Long.class))
            .isEqualTo(1);
        assertThat(http.getForEntity("/dead-letters?after=-1", String.class).getStatusCode().value()).isEqualTo(400);
        assertThat(http.postForEntity("/dead-letters/0/replay", null, String.class).getStatusCode().value()).isEqualTo(400);
    }

    @Test
    void startsWithMigratedDatabaseAndHealthyProbes() {
        for (String path : new String[] {"", "/liveness", "/readiness"}) {
            var response = http.getForEntity("/actuator/health" + path, String.class);
            assertThat(response.getStatusCode().value()).isEqualTo(200);
            assertThat(response.getBody()).contains("\"status\":\"UP\"");
        }
        assertThat(jdbc.queryForObject(
            "SELECT count(*) FROM flyway_schema_history WHERE version = '1' AND success", Integer.class))
            .isEqualTo(1);
        assertThat(jdbc.queryForObject(
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'order_service'", Integer.class))
            .isEqualTo(1);
    }

    @Test
    void failedBusinessInsertRollsBackItsOutboxEvent() {
        String trace = "rollback-create";
        assertThatThrownBy(() -> service.create("DEMO-SKU", 0, new BigDecimal("1.00"), trace))
            .isInstanceOf(RuntimeException.class);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.orders WHERE correlation_id = ?",
            Integer.class, trace)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.outbox_events WHERE correlation_id = ?",
            Integer.class, trace)).isZero();
    }

    @Test
    void failedConfirmationRollsBackStatusClaimAndOutboxAndCanBeRetried() {
        var order = service.create("DEMO-SKU", 1, new BigDecimal("1.00"), "rollback-confirm");
        var event = new EventEnvelope<>(UUID.randomUUID(), OrderEvents.PAYMENT_SUCCEEDED,
            order.correlationId(), Instant.now(), 1, order.data());
        jdbc.execute("ALTER TABLE order_service.orders ADD CONSTRAINT test_keep_created CHECK (status = 'CREATED')");
        try {
            assertThatThrownBy(() -> service.handle(event)).isInstanceOf(RuntimeException.class);
            assertThat(service.get(order.id()).status().name()).isEqualTo("CREATED");
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.processed_events WHERE event_id = ?",
                Integer.class, event.eventId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.outbox_events WHERE aggregate_id = ?",
                Integer.class, order.id())).isEqualTo(1);
        } finally {
            jdbc.execute("ALTER TABLE order_service.orders DROP CONSTRAINT test_keep_created");
        }
        service.handle(event);
        service.handle(event);
        assertThat(service.get(order.id()).status().name()).isEqualTo("CONFIRMED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.processed_events WHERE event_id = ?",
            Integer.class, event.eventId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM order_service.outbox_events WHERE aggregate_id = ?",
            Integer.class, order.id())).isEqualTo(2);
    }

    @Test
    void kafkaBrokerIsReachable() throws Exception {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            assertThat(admin.describeCluster().nodes().get(15, TimeUnit.SECONDS)).hasSize(1);
        }
    }
}
