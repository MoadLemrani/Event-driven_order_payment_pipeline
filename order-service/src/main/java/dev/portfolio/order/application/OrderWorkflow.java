package dev.portfolio.order.application;

import dev.portfolio.order.api.CreateOrderRequest;
import dev.portfolio.order.api.OrderResponse;
import org.springframework.stereotype.Service;
import java.util.UUID;

@Service
public class OrderWorkflow {
    private final OrderService orders;

    public OrderWorkflow(OrderService orders) {
        this.orders = orders;
    }

    public OrderResponse create(CreateOrderRequest request, String correlationId) {
        String trace = correlationId == null ? UUID.randomUUID().toString() : correlationId;
        return orders.create(request.sku(), request.quantity(), request.totalAmount(), trace);
    }
}
