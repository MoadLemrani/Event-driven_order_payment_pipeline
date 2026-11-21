package dev.portfolio.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

@Service
public class DeadLetterStore {
    private final JdbcTemplate jdbc;
    private final ObjectMapper json;

    public DeadLetterStore(JdbcTemplate jdbc, ObjectMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    @KafkaListener(topics = KafkaConfiguration.DEAD_LETTERS, groupId = "order-dead-letter-store")
    @Transactional
    public void capture(ConsumerRecord<String, String> record) {
        var error = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_MESSAGE);
        jdbc.update("""
            INSERT INTO order_service.dead_letters
              (topic, partition_number, record_offset, message_key, envelope, error_message)
            VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
            """, record.topic(), record.partition(), record.offset(), record.key(), record.value(),
            error == null ? null : new String(error.value(), StandardCharsets.UTF_8));
    }

    public List<DeadLetter> list(long after) {
        return jdbc.query("""
            SELECT * FROM order_service.dead_letters WHERE id > ? ORDER BY id LIMIT 100
            """, (rs, row) -> new DeadLetter(rs.getLong("id"), rs.getString("topic"),
                rs.getInt("partition_number"), rs.getLong("record_offset"), rs.getString("message_key"),
                rs.getString("envelope"), rs.getString("error_message"),
                rs.getTimestamp("received_at").toInstant(),
                rs.getTimestamp("replay_requested_at") == null ? null :
                    rs.getTimestamp("replay_requested_at").toInstant()), after);
    }

    @Transactional
    public void replay(long id) {
        var rows = jdbc.queryForList("SELECT * FROM order_service.dead_letters WHERE id = ? FOR UPDATE", id);
        if (rows.isEmpty()) throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Dead letter not found");
        var row = rows.getFirst();
        if (row.get("replay_requested_at") != null) return;
        // This UUID identifies the publication, not the event: the original envelope stays byte-for-byte intact.
        var publicationId = UUID.randomUUID();
        jdbc.update("""
            INSERT INTO order_service.outbox_events
              (event_id, aggregate_id, event_type, correlation_id, envelope, destination, message_key)
            VALUES (?, ?, 'DeadLetterReplay', ?, ?, ?, ?)
            """, publicationId, publicationId, correlationId(row.get("envelope"), id), row.get("envelope"),
            KafkaConfiguration.REPLAY, row.get("message_key"));
        jdbc.update("UPDATE order_service.dead_letters SET replay_requested_at = CURRENT_TIMESTAMP WHERE id = ?", id);
    }

    private String correlationId(Object envelope, long id) {
        if (envelope instanceof String raw) {
            try {
                var tree = json.readTree(raw);
                String trace = tree == null ? "" : tree.path("correlationId").asText();
                if (!trace.isBlank() && trace.length() <= 100) return trace;
            } catch (JsonProcessingException ignored) {
                // Malformed messages still need a traceable replay operation.
            }
        }
        return "replay-" + id;
    }

    public record DeadLetter(long id, String topic, int partition, long offset, String messageKey,
                             String envelope, String errorMessage, Instant receivedAt, Instant replayRequestedAt) {}
}
