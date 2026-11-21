package dev.portfolio.payment.application;

import dev.portfolio.events.OrderData;
import java.util.concurrent.ThreadLocalRandom;
import dev.portfolio.payment.domain.Payment;
import dev.portfolio.payment.domain.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {
    private final PaymentRepository payments;

    private final PaymentProperties properties;

    public PaymentService(PaymentRepository payments, PaymentProperties properties) {
        this.payments = payments;
        this.properties = properties;
    }

    @Transactional
    public boolean pay(OrderData order) {
        boolean succeeded = ThreadLocalRandom.current().nextDouble()
            >= properties.failureRate().doubleValue();
        payments.save(new Payment(order, succeeded));
        return succeeded;
    }
}
