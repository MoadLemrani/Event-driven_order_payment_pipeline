package dev.portfolio.inventory;

import org.junit.jupiter.api.Test;
import dev.portfolio.inventory.application.InventoryService;
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
class InventoryApplicationIT {
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
    private final InventoryService service;

    @Autowired
    InventoryApplicationIT(TestRestTemplate http, JdbcTemplate jdbc, InventoryService service) {
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
            "SELECT count(*) FROM information_schema.schemata WHERE schema_name = 'inventory_service'", Integer.class))
            .isEqualTo(1);
    }

    @Test
    void failedReservationRollsBackStockClaimAndOutboxAndCanBeRetried() {
        var data = new OrderData(UUID.randomUUID(), "DEMO-SKU", 1, new BigDecimal("1.00"));
        var event = new EventEnvelope<>(UUID.randomUUID(), OrderEvents.CREATED, "rollback-stock", Instant.now(), 1, data);
        jdbc.execute("ALTER TABLE inventory_service.stock ADD CONSTRAINT test_keep_stock CHECK (available >= 100)");
        try {
            assertThatThrownBy(() -> service.handle(event)).isInstanceOf(RuntimeException.class);
            assertThat(jdbc.queryForObject("SELECT available FROM inventory_service.stock WHERE sku = 'DEMO-SKU'",
                Integer.class)).isEqualTo(100);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.reservations WHERE order_id = ?",
                Integer.class, data.orderId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.processed_events WHERE event_id = ?",
                Integer.class, event.eventId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.outbox_events WHERE aggregate_id = ?",
                Integer.class, data.orderId())).isZero();
        } finally {
            jdbc.execute("ALTER TABLE inventory_service.stock DROP CONSTRAINT test_keep_stock");
        }
        service.handle(event);
        service.handle(event);
        assertThat(jdbc.queryForObject("SELECT available FROM inventory_service.stock WHERE sku = 'DEMO-SKU'",
            Integer.class)).isEqualTo(99);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.processed_events WHERE event_id = ?",
            Integer.class, event.eventId())).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.outbox_events WHERE aggregate_id = ?",
            Integer.class, data.orderId())).isEqualTo(1);
    }

    @Test
    void kafkaBrokerIsReachable() throws Exception {
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            assertThat(admin.describeCluster().nodes().get(15, TimeUnit.SECONDS)).hasSize(1);
        }
    }
}
