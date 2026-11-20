package dev.portfolio.order.api;

import dev.portfolio.order.domain.Order;
import dev.portfolio.order.domain.OrderStatus;
import dev.portfolio.events.OrderData;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(UUID id, String sku, int quantity, BigDecimal totalAmount,
                            String currency, OrderStatus status, String correlationId, Instant createdAt) {
    public static OrderResponse from(Order order) {
        return new OrderResponse(order.getId(), order.getSku(), order.getQuantity(), order.getTotalAmount(),
            "USD", order.getStatus(), order.getCorrelationId(), order.getCreatedAt());
    }

    public OrderData data() { return new OrderData(id, sku, quantity, totalAmount); }
}
