package dev.portfolio.payment;

import dev.portfolio.payment.application.PaymentProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@EnableConfigurationProperties(PaymentProperties.class)
@SpringBootApplication
public class PaymentApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}
