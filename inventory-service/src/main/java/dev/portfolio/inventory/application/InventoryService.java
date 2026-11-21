package dev.portfolio.inventory.application;

import dev.portfolio.events.OrderData;
import java.util.UUID;
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
    public boolean reserve(OrderData order) {
        var item = stock.findById(order.sku());
        if (item.isEmpty() || !item.get().reserve(order.quantity())) {
            return false;
        }
        reservations.save(new Reservation(order));
        return true;
    }

    @Transactional
    public void release(UUID orderId) {
        var reservation = reservations.findById(orderId)
            .orElseThrow(() -> new IllegalStateException("Missing reservation for " + orderId));
        if (reservation.release()) {
            // Restore the recorded reservation, never an amount supplied by a later event.
            var item = stock.findById(reservation.getSku()).orElseThrow();
            item.release(reservation.getQuantity());
        }
    }
}
