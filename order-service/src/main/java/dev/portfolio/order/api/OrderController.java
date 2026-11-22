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
            .body(order);
    }

    @GetMapping
    public java.util.List<OrderResponse> recent() { return orders.recent(); }

    @GetMapping("/{id}/timeline")
    public java.util.List<OrderService.TimelineEvent> timeline(@PathVariable UUID id) { return orders.timeline(id); }

    @GetMapping("/{id}")
    public ResponseEntity<OrderResponse> get(@PathVariable UUID id, jakarta.servlet.http.HttpServletResponse response) {
        var order = orders.get(id);
        org.slf4j.MDC.put("correlationId", order.correlationId());
        response.setHeader("X-Correlation-ID", order.correlationId());
        return ResponseEntity.ok().body(order);
    }
}
