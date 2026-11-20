package dev.portfolio.payment.application;

import dev.portfolio.events.OrderData;
import dev.portfolio.payment.domain.Payment;
import dev.portfolio.payment.domain.PaymentRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class PaymentService {
    private final PaymentRepository payments;

    public PaymentService(PaymentRepository payments) { this.payments = payments; }

    @Transactional
    public void pay(OrderData order) { payments.save(new Payment(order)); }
}
