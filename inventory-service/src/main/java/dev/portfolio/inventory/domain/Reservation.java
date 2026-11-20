package dev.portfolio.inventory.domain;

import dev.portfolio.events.OrderData;
import jakarta.persistence.*;
import java.util.UUID;

@Entity
@Table(name = "reservations", schema = "inventory_service")
public class Reservation {
    @Id private UUID orderId;
    @Column(nullable = false, length = 64) private String sku;
    @Column(nullable = false) private int quantity;

    protected Reservation() {}

    public Reservation(OrderData order) {
        orderId = order.orderId();
        sku = order.sku();
        quantity = order.quantity();
    }
}
