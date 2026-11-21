package dev.portfolio.order.messaging;

import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
@ConditionalOnProperty(name = "outbox.publisher-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {
    private final JdbcTemplate jdbc;
    private final KafkaTemplate<String, String> kafka;

    public OutboxPublisher(JdbcTemplate jdbc, KafkaTemplate<String, String> kafka) {
        this.jdbc = jdbc;
        this.kafka = kafka;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:250}")
    @Transactional
    public void publishPending() {
        // One active publisher per service database, even if several application instances poll.
        // Transaction-scoped locks are released automatically on commit, rollback, or process death.
        if (!Boolean.TRUE.equals(jdbc.queryForObject(
                "SELECT pg_try_advisory_xact_lock(730401::bigint)", Boolean.class))) {
            return;
        }
        var batch = jdbc.query("""
            SELECT id, CASE WHEN destination = 'order-events' THEN aggregate_id::text ELSE message_key END AS message_key, destination, correlation_id, envelope FROM order_service.outbox_events
            WHERE sent_at IS NULL ORDER BY id LIMIT 50 FOR UPDATE
            """, (rs, row) -> new PendingEvent(rs.getLong("id"), rs.getString("message_key"), rs.getString("destination"),
                rs.getString("correlation_id"), rs.getString("envelope")));
        for (var event : batch) {
            try (var ignored = MDC.putCloseable("correlationId", event.correlationId())) {
                // Reuse the exact serialized envelope and eventId on every attempt.
                kafka.send(event.destination(), event.messageKey(), event.envelope()).get(15, TimeUnit.SECONDS);
                jdbc.update("UPDATE order_service.outbox_events SET sent_at = CURRENT_TIMESTAMP WHERE id = ?",
                    event.id());
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Outbox publication interrupted", ex);
            } catch (ExecutionException | TimeoutException ex) {
                // Roll back sent markers. Already acknowledged messages may be redelivered safely.
                throw new IllegalStateException("Outbox publication failed; pending events will be retried", ex);
            }
        }
    }

    private record PendingEvent(long id, String messageKey, String destination, String correlationId, String envelope) {}
}
