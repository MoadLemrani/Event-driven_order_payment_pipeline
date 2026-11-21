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

    public OrderService(OrderRepository orders, EventOutbox outbox, ProcessedEvents processed) {
        this.orders = orders;
        this.outbox = outbox;
        this.processed = processed;
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
            default -> throw new IllegalArgumentException("Unexpected event " + event.eventType());
        }
        outbox.append(outgoing, order.getCorrelationId(), order.data());
    }

    private Order find(UUID id) {
        return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
}
