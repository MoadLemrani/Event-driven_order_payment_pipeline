package dev.portfolio.inventory.messaging;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import java.util.UUID;

@Component
public class ProcessedEvents {
    private final JdbcTemplate jdbc;

    public ProcessedEvents(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    /** The unique insert serializes duplicate claims; a failed business transaction undoes the claim. */
    @Transactional(propagation = Propagation.MANDATORY)
    public boolean claim(UUID eventId) {
        return jdbc.update("""
            INSERT INTO inventory_service.processed_events (event_id) VALUES (?)
            ON CONFLICT (event_id) DO NOTHING
            """, eventId) == 1;
    }
}
