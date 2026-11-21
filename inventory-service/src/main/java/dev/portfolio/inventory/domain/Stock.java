package dev.portfolio.inventory.domain;

import jakarta.persistence.*;

@Entity
@Table(name = "stock", schema = "inventory_service")
public class Stock {
    @Id @Column(length = 64) private String sku;
    @Column(nullable = false) private int available;

    protected Stock() {}

    public boolean reserve(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Reservation quantity must be positive");
        }
        if (available < quantity) {
            return false;
        }
        available -= quantity;
        return true;
    }

    public void release(int quantity) {
        if (quantity <= 0) {
            throw new IllegalArgumentException("Release quantity must be positive");
        }
        available = Math.addExact(available, quantity);
    }
}
