package dev.portfolio.order.application;

import dev.portfolio.order.api.OrderResponse;
import dev.portfolio.order.domain.Order;
import dev.portfolio.order.domain.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;

@Service
public class OrderService {
    private final OrderRepository orders;

    public OrderService(OrderRepository orders) { this.orders = orders; }

    @Transactional
    public OrderResponse create(String sku, int quantity, BigDecimal totalAmount, String correlationId) {
        return OrderResponse.from(orders.save(new Order(sku, quantity, totalAmount, correlationId)));
    }

    @Transactional(readOnly = true)
    public OrderResponse get(UUID id) { return OrderResponse.from(find(id)); }

    @Transactional
    public OrderResponse confirm(UUID id) {
        var order = find(id);
        order.confirm();
        return OrderResponse.from(order);
    }

    private Order find(UUID id) {
        return orders.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }
}
