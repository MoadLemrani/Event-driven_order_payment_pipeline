package dev.portfolio.order.api;

import dev.portfolio.order.application.OrderService;
import dev.portfolio.order.application.OrderWorkflow;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Pattern;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.net.URI;
import java.util.UUID;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final OrderWorkflow workflow;
    private final OrderService orders;

    public OrderController(OrderWorkflow workflow, OrderService orders) {
        this.workflow = workflow;
        this.orders = orders;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> create(@Valid @RequestBody CreateOrderRequest request,
            @RequestHeader(value = "X-Correlation-ID", required = false)
            @Pattern(regexp = "[A-Za-z0-9._-]{1,100}") String correlationId) {
        var order = workflow.create(request, correlationId);
        return ResponseEntity.created(URI.create("/orders/" + order.id()))
            .header("X-Correlation-ID", order.correlationId()).body(order);
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable UUID id) {
        var order = orders.get(id);
        return ResponseEntity.ok().header("X-Correlation-ID", order.correlationId()).body(order);
    }
}
