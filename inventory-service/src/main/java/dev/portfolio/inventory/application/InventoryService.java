package dev.portfolio.inventory.application;

import dev.portfolio.events.OrderData;
import dev.portfolio.events.EventEnvelope;
import dev.portfolio.events.OrderEvents;
import dev.portfolio.inventory.messaging.EventOutbox;
import dev.portfolio.inventory.messaging.ProcessedEvents;
import java.util.UUID;
import dev.portfolio.inventory.domain.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class InventoryService {
    private final StockRepository stock;
    private final ReservationRepository reservations;

    private final EventOutbox outbox;
    private final ProcessedEvents processed;

    public InventoryService(StockRepository stock, ReservationRepository reservations,
                            EventOutbox outbox, ProcessedEvents processed) {
        this.stock = stock;
        this.reservations = reservations;
        this.outbox = outbox;
        this.processed = processed;
    }

    @Transactional
    public void handle(EventEnvelope<OrderData> event) {
        if (!processed.claim(event.eventId())) {
            return;
        }
        String outgoing;
        switch (event.eventType()) {
            case OrderEvents.CREATED -> outgoing = reserve(event.payload())
                ? OrderEvents.INVENTORY_RESERVED : OrderEvents.INVENTORY_FAILED;
            case OrderEvents.PAYMENT_FAILED -> {
                release(event.payload().orderId());
                outgoing = OrderEvents.INVENTORY_RELEASED;
            }
            default -> throw new IllegalArgumentException("Unexpected event " + event.eventType());
        }
        outbox.append(outgoing, event.correlationId(), event.payload());
    }

    private boolean reserve(OrderData order) {
        var item = stock.findById(order.sku());
        if (item.isEmpty() || !item.get().reserve(order.quantity())) {
            return false;
        }
        reservations.save(new Reservation(order));
        return true;
    }

    private void release(UUID orderId) {
        var reservation = reservations.findById(orderId)
            .orElseThrow(() -> new IllegalStateException("Missing reservation for " + orderId));
        if (reservation.release()) {
            // Restore the recorded reservation, never an amount supplied by a later event.
            var item = stock.findById(reservation.getSku()).orElseThrow();
            item.release(reservation.getQuantity());
        }
    }
}
