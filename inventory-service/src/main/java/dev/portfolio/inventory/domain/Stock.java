package dev.portfolio.inventory.domain;

import jakarta.persistence.*;

@Entity
@Table(name = "stock", schema = "inventory_service")
public class Stock {
    @Id @Column(length = 64) private String sku;
    @Column(nullable = false) private int available;

    protected Stock() {}

    public void reserve(int quantity) {
        if (quantity <= 0 || available < quantity) {
            throw new IllegalStateException("Insufficient stock for " + sku);
        }
        available -= quantity;
    }
}
