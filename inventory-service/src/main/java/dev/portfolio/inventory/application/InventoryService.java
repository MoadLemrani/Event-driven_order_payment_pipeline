package dev.portfolio.inventory.application;

import dev.portfolio.events.OrderData;
import dev.portfolio.inventory.domain.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {
    private final StockRepository stock;
    private final ReservationRepository reservations;

    public InventoryService(StockRepository stock, ReservationRepository reservations) {
        this.stock = stock;
        this.reservations = reservations;
    }

    @Transactional
    public void reserve(OrderData order) {
        var item = stock.findById(order.sku())
            .orElseThrow(() -> new IllegalStateException("Unknown SKU " + order.sku()));
        item.reserve(order.quantity());
        reservations.save(new Reservation(order));
    }
}
