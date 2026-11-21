package dev.portfolio.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.portfolio.events.EventEnvelope;
import dev.portfolio.events.OrderData;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.UUID;

@Component
public class EventOutbox {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public EventOutbox(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /** Never commit an event independently of the business transaction. */
    @Transactional(propagation = Propagation.MANDATORY)
    public void append(String type, String correlationId, OrderData data) {
        var event = new EventEnvelope<>(UUID.randomUUID(), type, correlationId, Instant.now(), 1, data);
        try {
            jdbc.update("""
                INSERT INTO order_service.outbox_events
                    (event_id, aggregate_id, event_type, correlation_id, envelope)
                VALUES (?, ?, ?, ?, ?)
                """, event.eventId(), data.orderId(), type, correlationId, json.writeValueAsString(event));
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Cannot serialize outbox event", ex);
        }
    }
}
