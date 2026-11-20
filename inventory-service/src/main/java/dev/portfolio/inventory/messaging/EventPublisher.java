package dev.portfolio.inventory.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.portfolio.events.EventEnvelope;
import dev.portfolio.events.OrderData;
import dev.portfolio.events.OrderEvents;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Direct publishing is temporary: Phase 4 replaces the commit/publish gap with an outbox. */
@Component
public class EventPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper json;

    public EventPublisher(KafkaTemplate<String, String> kafka, ObjectMapper json) {
        this.kafka = kafka;
        this.json = json;
    }

    public void publish(String eventType, String correlationId, OrderData data) {
        var event = new EventEnvelope<>(UUID.randomUUID(), eventType, correlationId, Instant.now(), 1, data);
        try {
            kafka.send(OrderEvents.TOPIC, data.orderId().toString(), json.writeValueAsString(event))
                .get(15, TimeUnit.SECONDS);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new PublicationException(data.orderId(), ex);
        } catch (JsonProcessingException | ExecutionException | TimeoutException ex) {
            throw new PublicationException(data.orderId(), ex);
        }
    }

    public static class PublicationException extends RuntimeException {
        public PublicationException(UUID orderId, Exception cause) {
            super("Order " + orderId + " was saved, but event publication could not be confirmed", cause);
        }
    }
}
