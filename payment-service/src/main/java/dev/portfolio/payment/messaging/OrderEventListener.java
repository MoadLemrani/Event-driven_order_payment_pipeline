package dev.portfolio.payment.messaging;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.portfolio.events.*;
import dev.portfolio.payment.application.PaymentService;
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
    private final PaymentService service;
    private final EventPublisher events;

    public OrderEventListener(ObjectMapper json, PaymentService service, EventPublisher events) {
        this.json = json;
        this.service = service;
        this.events = events;
    }

    @KafkaListener(topics = OrderEvents.TOPIC)
    public void onEvent(String message) throws JsonProcessingException {
        EventEnvelope<OrderData> event = json.readValue(message, EVENT_TYPE);
        if (!OrderEvents.INVENTORY_RESERVED.equals(event.eventType())) {
            return;
        }
        if (event.version() != 1) {
            throw new IllegalArgumentException("Unsupported event version " + event.version());
        }
        try (var ignored = MDC.putCloseable("correlationId", event.correlationId())) {
            service.pay(event.payload());
            events.publish(OrderEvents.PAYMENT_SUCCEEDED, event.correlationId(), event.payload());
            log.info("Processed {} for order {}", event.eventType(), event.payload().orderId());
        }
    }
}
