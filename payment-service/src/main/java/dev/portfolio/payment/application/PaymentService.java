package dev.portfolio.payment.application;

import dev.portfolio.events.OrderData;
import dev.portfolio.events.EventEnvelope;
import dev.portfolio.events.OrderEvents;
import dev.portfolio.payment.messaging.EventOutbox;
import dev.portfolio.payment.messaging.ProcessedEvents;
import java.util.concurrent.ThreadLocalRandom;
import dev.portfolio.payment.domain.Payment;
import dev.portfolio.payment.domain.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {
    private final PaymentRepository payments;
    private final PaymentControl control;
    private final EventOutbox outbox;
    private final ProcessedEvents processed;

    public PaymentService(PaymentRepository payments, PaymentControl control,
                          EventOutbox outbox, ProcessedEvents processed) {
        this.payments = payments;
        this.control = control;
        this.outbox = outbox;
        this.processed = processed;
    }

    @Transactional
    public void handle(EventEnvelope<OrderData> event) {
        if (!processed.claim(event.eventId())) {
            return;
        }
        if (!OrderEvents.INVENTORY_RESERVED.equals(event.eventType())) {
            throw new IllegalArgumentException("Unexpected event " + event.eventType());
        }
        boolean succeeded = ThreadLocalRandom.current().nextDouble()
            >= control.failureRate().doubleValue();
        payments.save(new Payment(event.payload(), succeeded));
        outbox.append(succeeded ? OrderEvents.PAYMENT_SUCCEEDED : OrderEvents.PAYMENT_FAILED,
            event.correlationId(), event.payload());
    }
}
