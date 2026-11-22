package dev.portfolio.order.observability;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class PipelineMetrics {
    private final JdbcTemplate jdbc;
    public PipelineMetrics(JdbcTemplate jdbc, MeterRegistry registry) {
        this.jdbc = jdbc;
        Gauge.builder("pipeline.dlq.pending", this, m -> m.count(
            "SELECT count(*) FROM order_service.dead_letters WHERE replay_requested_at IS NULL"))
            .description("Persisted dead letters without a replay request").register(registry);
        Gauge.builder("pipeline.outbox.pending", this, m -> m.count(
            "SELECT count(*) FROM order_service.outbox_events WHERE sent_at IS NULL"))
            .register(registry);
        FunctionCounter.builder("pipeline.orders", this, m -> m.count("SELECT count(*) FROM order_service.orders"))
            .tag("outcome", "created")
            .description("Durable total of orders by lifecycle outcome").register(registry);
        for (String status : new String[] {"CONFIRMED", "CANCELLED"}) {
            FunctionCounter.builder("pipeline.orders", this,
                m -> m.count("SELECT count(*) FROM order_service.orders WHERE status = '" + status + "'"))
                .tag("outcome", status.toLowerCase(java.util.Locale.ROOT))
                .description("Durable total of orders by lifecycle outcome").register(registry);
        }
    }
    private double count(String sql) { return jdbc.queryForObject(sql, Long.class); }
}
