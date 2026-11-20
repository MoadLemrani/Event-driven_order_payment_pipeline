package dev.portfolio.events;

import org.junit.jupiter.api.Test;
import java.time.Instant;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class EventEnvelopeTest {
    @Test
    void preservesTraceAndPayload() {
        var id = UUID.randomUUID();
        var event = new EventEnvelope<>(id, "OrderCreated", "trace-1", Instant.now(), 1, "payload");
        assertEquals(id, event.eventId());
        assertEquals("trace-1", event.correlationId());
        assertEquals("payload", event.payload());
    }

    @Test
    void rejectsUnversionedOrUntraceableEvents() {
        assertThrows(IllegalArgumentException.class, () ->
            new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", "trace", Instant.now(), 0, "payload"));
        assertThrows(IllegalArgumentException.class, () ->
            new EventEnvelope<>(UUID.randomUUID(), "OrderCreated", " ", Instant.now(), 1, "payload"));
    }
}
