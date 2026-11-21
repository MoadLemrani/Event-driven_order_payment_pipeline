package dev.portfolio.payment;

import org.junit.jupiter.api.Test;
import dev.portfolio.payment.application.PaymentService;
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

@Testcontainers
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class PaymentApplicationIT {
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
        registry.add("payment.failure-rate", () -> "0");
    }

    private final TestRestTemplate http;
    private final JdbcTemplate jdbc;
    private final PaymentService service;

    @Autowired
    PaymentApplicationIT(TestRestTemplate http, JdbcTemplate jdbc, PaymentService service) {
        this.http = http;
        this.jdbc = jdbc;
        this.service = service;
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
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'payment_service'", Integer.class))
            .isEqualTo(1);
    }

    @Test
    void failedPaymentWriteRollsBackClaimAndOutboxAndCanBeRetried() {
        var data = new OrderData(UUID.randomUUID(), "DEMO-SKU", 1, new BigDecimal("1.00"));
        var event = new EventEnvelope<>(UUID.randomUUID(), OrderEvents.INVENTORY_RESERVED,
            "rollback-payment", Instant.now(), 1, data);
        jdbc.execute("ALTER TABLE payment_service.payments ADD CONSTRAINT test_reject_success CHECK (status = 'FAILED')");
        try {
            assertThatThrownBy(() -> service.handle(event)).isInstanceOf(RuntimeException.class);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_service.payments WHERE order_id = ?",
                Integer.class, data.orderId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_service.processed_events WHERE event_id = ?",
                Integer.class, event.eventId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_service.outbox_events WHERE aggregate_id = ?",
                Integer.class, data.orderId())).isZero();
        } finally {
            jdbc.execute("ALTER TABLE payment_service.payments DROP CONSTRAINT test_reject_success");
        }
        service.handle(event);
        service.handle(event);
        assertThat(jdbc.queryForObject("SELECT status FROM payment_service.payments WHERE order_id = ?",
            String.class, data.orderId())).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_service.processed_events WHERE event_id = ?",
            Integer.class, event.eventId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM payment_service.outbox_events WHERE aggregate_id = ?",
            Integer.class, data.orderId())).isEqualTo(1);
    }

    @Test
    void kafkaBrokerIsReachable() throws Exception {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            assertThat(admin.describeCluster().nodes().get(15, TimeUnit.SECONDS)).hasSize(1);
        }
    }
}
