package dev.portfolio.order.application;

import dev.portfolio.events.EventEnvelope;
import dev.portfolio.events.OrderData;
import dev.portfolio.events.OrderEvents;
import dev.portfolio.order.api.OrderResponse;
import dev.portfolio.order.domain.Order;
import dev.portfolio.order.domain.OrderRepository;
import dev.portfolio.order.messaging.EventOutbox;
import dev.portfolio.order.messaging.ProcessedEvents;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {
    private final OrderRepository orders;
    private final EventOutbox outbox;
    private final ProcessedEvents processed;
    private final org.springframework.jdbc.core.JdbcTemplate jdbc;

    public OrderService(OrderRepository orders, EventOutbox outbox, ProcessedEvents processed, org.springframework.jdbc.core.JdbcTemplate jdbc) {
        this.orders = orders;
        this.outbox = outbox;
        this.processed = processed;
        this.jdbc = jdbc;
    }

    @Transactional
    public OrderResponse create(String sku, int quantity, BigDecimal totalAmount, String correlationId) {
        var order = orders.save(new Order(sku, quantity, totalAmount, correlationId));
        outbox.append(OrderEvents.CREATED, correlationId, order.data());
        return OrderResponse.from(order);
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID id) { return OrderResponse.from(find(id)); }

    @Transactional
    public void handle(EventEnvelope<OrderData> event) {
        if (!processed.claim(event.eventId())) {
            return;
        }
        var order = find(event.payload().orderId());
        jdbc.update("""
            INSERT INTO order_service.order_timeline (event_id, order_id, event_type, correlation_id, occurred_at)
            VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING
            """, event.eventId(), order.getId(), event.eventType(), event.correlationId(),
            java.sql.Timestamp.from(event.occurredAt()));
        String outgoing;
        switch (event.eventType()) {
            case OrderEvents.PAYMENT_SUCCEEDED -> {
                order.confirm();
                outgoing = OrderEvents.CONFIRMED;
            }
            case OrderEvents.INVENTORY_FAILED, OrderEvents.INVENTORY_RELEASED -> {
                order.cancel();
                outgoing = OrderEvents.CANCELLED;
            }
            default -> { return; }
        }
        outbox.append(outgoing, order.getCorrelationId(), order.data());
    }

    @Transactional(readOnly = true)
    public java.util.List<OrderResponse> recent() {
        return orders.findAll(org.springframework.data.domain.PageRequest.of(0, 100,
            org.springframework.data.domain.Sort.by(org.springframework.data.domain.Sort.Direction.DESC, "createdAt")))
            .stream().map(OrderResponse::from).toList();
    }

    @Transactional(readOnly = true)
    public java.util.List<TimelineEvent> timeline(UUID id) {
        find(id);
        return jdbc.query("""
            SELECT event_id, event_type, correlation_id, occurred_at FROM order_service.order_timeline
            WHERE order_id = ? ORDER BY recorded_at, event_id
            """, (rs, n) -> new TimelineEvent(rs.getObject("event_id", UUID.class), rs.getString("event_type"),
                rs.getString("correlation_id"), rs.getTimestamp("occurred_at").toInstant()), id);
    }

    public record TimelineEvent(UUID eventId, String eventType, String correlationId, java.time.Instant occurredAt) {}

    private Order find(UUID id) {
        return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
}
