package dev.portfolio.order.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.portfolio.events.*;
import dev.portfolio.order.application.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderEventListener {
    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);
    private static final TypeReference<EventEnvelope<OrderData>> EVENT_TYPE = new TypeReference<>() {};
    private final ObjectMapper json;
    private final OrderService service;
    private final EventPublisher events;

    public OrderEventListener(ObjectMapper json, OrderService service, EventPublisher events) {
        this.json = json;
        this.service = service;
        this.events = events;
    }

    @KafkaListener(topics = OrderEvents.TOPIC)
    public void onEvent(String message) throws JsonProcessingException {
        EventEnvelope<OrderData> event = json.readValue(message, EVENT_TYPE);
        if (!OrderEvents.PAYMENT_SUCCEEDED.equals(event.eventType())
                && !OrderEvents.INVENTORY_FAILED.equals(event.eventType())
                && !OrderEvents.INVENTORY_RELEASED.equals(event.eventType())) {
            return;
        }
        if (event.version() != 1) {
            throw new IllegalArgumentException("Unsupported event version " + event.version());
        }
        try (var ignored = MDC.putCloseable("correlationId", event.correlationId())) {
            if (OrderEvents.PAYMENT_SUCCEEDED.equals(event.eventType())) {
                var confirmed = service.confirm(event.payload().orderId());
                events.publish(OrderEvents.CONFIRMED, confirmed.correlationId(), confirmed.data());
            } else {
                // Payment failure becomes terminal only after inventory confirms compensation.
                var cancelled = service.cancel(event.payload().orderId());
                events.publish(OrderEvents.CANCELLED, cancelled.correlationId(), cancelled.data());
            }
            log.info("Processed {} for order {}", event.eventType(), event.payload().orderId());
        }
    }
}
