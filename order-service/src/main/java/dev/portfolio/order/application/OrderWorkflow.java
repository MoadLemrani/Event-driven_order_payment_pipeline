package dev.portfolio.order.application;

import dev.portfolio.events.OrderEvents;
import dev.portfolio.order.api.CreateOrderRequest;
import dev.portfolio.order.api.OrderResponse;
import dev.portfolio.order.messaging.EventPublisher;
import org.springframework.stereotype.Service;
import java.util.UUID;

@Service
public class OrderWorkflow {
    private final OrderService orders;
    private final EventPublisher events;

    public OrderWorkflow(OrderService orders, EventPublisher events) {
        this.orders = orders;
        this.events = events;
    }

    public OrderResponse create(CreateOrderRequest request, String correlationId) {
        String trace = correlationId == null ? UUID.randomUUID().toString() : correlationId;
        var order = orders.create(request.sku(), request.quantity(), request.totalAmount(), trace);
        events.publish(OrderEvents.CREATED, trace, order.data());
        return order;
    }
}
