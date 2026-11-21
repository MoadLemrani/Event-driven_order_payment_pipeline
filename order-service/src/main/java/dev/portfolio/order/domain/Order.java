package dev.portfolio.order.domain;

import dev.portfolio.events.OrderData;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders", schema = "order_service")
public class Order {
    @Id private UUID id;
    @Column(nullable = false, length = 64) private String sku;
    @Column(nullable = false) private int quantity;
    @Column(nullable = false, precision = 14, scale = 2) private BigDecimal totalAmount;
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20) private OrderStatus status;
    @Column(nullable = false, length = 100) private String correlationId;
    @Column(nullable = false) private Instant createdAt;

    protected Order() {}

    public Order(String sku, int quantity, BigDecimal totalAmount, String correlationId) {
        this.id = UUID.randomUUID();
        this.sku = sku;
        this.quantity = quantity;
        this.totalAmount = totalAmount;
        this.correlationId = correlationId;
        this.status = OrderStatus.CREATED;
        this.createdAt = Instant.now();
    }

    public void confirm() { transitionTo(OrderStatus.CONFIRMED); }
    public void cancel() { transitionTo(OrderStatus.CANCELLED); }

    private void transitionTo(OrderStatus target) {
        if (status != OrderStatus.CREATED && status != target) {
            throw new IllegalStateException("Cannot change terminal order status " + status + " to " + target);
        }
        status = target;
    }
    public OrderData data() { return new OrderData(id, sku, quantity, totalAmount); }
    public UUID getId() { return id; }
    public String getSku() { return sku; }
    public int getQuantity() { return quantity; }
    public BigDecimal getTotalAmount() { return totalAmount; }
    public OrderStatus getStatus() { return status; }
    public String getCorrelationId() { return correlationId; }
    public Instant getCreatedAt() { return createdAt; }
}
