package dev.portfolio.payment.domain;

import dev.portfolio.events.OrderData;
import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments", schema = "payment_service")
public class Payment {
    @Id private UUID orderId;
    @Column(nullable = false, precision = 14, scale = 2) private BigDecimal totalAmount;
    @Column(nullable = false, length = 20) private String status;
    @Column(nullable = false) private Instant processedAt;

    protected Payment() {}

    public Payment(OrderData order) {
        orderId = order.orderId();
        totalAmount = order.totalAmount();
        status = "SUCCEEDED";
        processedAt = Instant.now();
    }
}
