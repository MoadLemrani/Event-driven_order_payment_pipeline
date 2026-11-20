package dev.portfolio.events;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/** Transport-neutral versioned event contract shared by all services. */
public record EventEnvelope<T>(UUID eventId, String eventType, String correlationId,
                               Instant occurredAt, int version, T payload) {
    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId");
        Objects.requireNonNull(occurredAt, "occurredAt");
        Objects.requireNonNull(payload, "payload");
        if (eventType == null || eventType.isBlank()) {
            throw new IllegalArgumentException("eventType must not be blank");
        }
        if (correlationId == null || correlationId.isBlank()) {
            throw new IllegalArgumentException("correlationId must not be blank");
        }
        if (version < 1) {
            throw new IllegalArgumentException("version must be positive");
        }
    }
}
