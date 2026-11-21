package dev.portfolio.inventory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import dev.portfolio.inventory.domain.StockRepository;
import dev.portfolio.inventory.domain.ReservationRepository;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.dao.OptimisticLockingFailureException;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
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
    private final StockRepository stocks;
    private final ReservationRepository reservations;
    private final TransactionTemplate transactions;

    @Autowired
    InventoryApplicationIT(TestRestTemplate http, JdbcTemplate jdbc, InventoryService service,
                           StockRepository stocks, ReservationRepository reservations,
                           PlatformTransactionManager transactionManager) {
        this.http = http;
        this.jdbc = jdbc;
        this.service = service;
        this.stocks = stocks;
        this.reservations = reservations;
        this.transactions = new TransactionTemplate(transactionManager);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void staleReservationOrReleaseRollsBackAllEffectsAndRetriesSafely(boolean releasing) throws Exception {
        String sku = "RACE-" + UUID.randomUUID();
        jdbc.update("INSERT INTO inventory_service.stock (sku, available) VALUES (?, 1)", sku);
        var firstData = new OrderData(UUID.randomUUID(), sku, 1, BigDecimal.ONE);
        if (releasing) service.handle(event(OrderEvents.CREATED, firstData));
        var secondData = releasing ? firstData : new OrderData(UUID.randomUUID(), sku, 1, BigDecimal.ONE);
        String type = releasing ? OrderEvents.PAYMENT_FAILED : OrderEvents.CREATED;
        var events = List.of(event(type, firstData), event(type, secondData));
        var barrier = new CyclicBarrier(2);
        try (var executor = Executors.newFixedThreadPool(2)) {
            var futures = events.stream().map(event -> executor.submit(() -> {
                try {
                    transactions.executeWithoutResult(status -> {
                        // Each transaction must load the same version before either can mutate it.
                        stocks.findById(sku).orElseThrow();
                        if (releasing) reservations.findById(firstData.orderId()).orElseThrow();
                        try { barrier.await(10, TimeUnit.SECONDS); }
                        catch (Exception ex) { throw new IllegalStateException(ex); }
                        service.handle(event);
                    });
                    return true;
                } catch (OptimisticLockingFailureException expected) {
                    return false;
                }
            })).toList();
            boolean first = futures.get(0).get(20, TimeUnit.SECONDS);
            boolean second = futures.get(1).get(20, TimeUnit.SECONDS);
            assertThat(first).isNotEqualTo(second);
            var loser = events.get(first ? 1 : 0);
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.processed_events WHERE event_id = ?",
                Long.class, loser.eventId())).isZero();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.outbox_events WHERE envelope::jsonb->'payload'->>'sku' = ?",
                Long.class, sku)).isEqualTo(releasing ? 2 : 1);
            if (!releasing) {
                assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.reservations WHERE order_id = ?",
                    Long.class, loser.payload().orderId())).isZero();
            }
            // A fresh transaction reads the winner's version; the winner's duplicate is a no-op.
            events.forEach(service::handle);
        }
        assertThat(jdbc.queryForObject("SELECT available FROM inventory_service.stock WHERE sku = ?", Integer.class, sku))
            .isEqualTo(releasing ? 1 : 0);
        assertThat(jdbc.queryForObject("SELECT version FROM inventory_service.stock WHERE sku = ?", Long.class, sku))
            .isEqualTo(releasing ? 2 : 1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.reservations WHERE sku = ?", Long.class, sku))
            .isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.outbox_events WHERE envelope::jsonb->'payload'->>'sku' = ?",
            Long.class, sku)).isEqualTo(releasing ? 3 : 2);
        if (releasing) {
            assertThat(jdbc.queryForObject("SELECT version FROM inventory_service.reservations WHERE order_id = ?",
                Long.class, firstData.orderId())).isEqualTo(1);
        } else {
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.outbox_events WHERE event_type = 'InventoryFailed' AND envelope::jsonb->'payload'->>'sku' = ?",
                Long.class, sku)).isEqualTo(1);
        }
        for (var event : events) {
            assertThat(jdbc.queryForObject("SELECT count(*) FROM inventory_service.processed_events WHERE event_id = ?",
                Long.class, event.eventId())).isEqualTo(1);
        }
    }

    private static EventEnvelope<OrderData> event(String type, OrderData data) {
        return new EventEnvelope<>(UUID.randomUUID(), type, "inventory-race-" + data.orderId(), Instant.now(), 1, data);
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
        jdbc.execute("ALTER TABLE inventory_service.stock ADD CONSTRAINT test_keep_stock CHECK (sku <> 'DEMO-SKU' OR available >= 100)");
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
