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

    public OrderEventListener(ObjectMapper json, PaymentService service) {
        this.json = json;
        this.service = service;
    }

    @KafkaListener(topics = {OrderEvents.TOPIC, KafkaConfiguration.REPLAY})
    public void onEvent(String message) throws JsonProcessingException {
        EventEnvelope<OrderData> event = json.readValue(message, EVENT_TYPE);
        if (!OrderEvents.INVENTORY_RESERVED.equals(event.eventType())) {
            return;
        }
        try (var ignored = MDC.putCloseable("correlationId", event.correlationId())) {
            try {
                if (event.version() != 1) throw new IllegalArgumentException("Unsupported event version " + event.version());
                service.handle(event);
                log.info("Handled {} for order {}", event.eventType(), event.payload().orderId());
            } catch (RuntimeException ex) {
                log.warn("Event {} failed for order {}", event.eventId(), event.payload().orderId(), ex);
                throw ex;
            }
        }
    }
}
