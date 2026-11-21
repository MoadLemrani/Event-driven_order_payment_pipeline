package dev.portfolio.payment;

import dev.portfolio.payment.application.PaymentProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.TestConfiguration;
import static org.assertj.core.api.Assertions.assertThat;

class PaymentPropertiesTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
        .withUserConfiguration(PropertiesConfiguration.class);

    @ParameterizedTest
    @ValueSource(strings = {"0", "0.25", "1"})
    void acceptsProbabilitiesIncludingDeterministicBoundaries(String rate) {
        context.withPropertyValues("payment.failure-rate=" + rate).run(result -> {
            assertThat(result).hasNotFailed();
            assertThat(result.getBean(PaymentProperties.class).failureRate()).isEqualByComparingTo(rate);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"-0.01", "1.01", "NaN", "invalid"})
    void rejectsInvalidFailureRatesAtStartup(String rate) {
        context.withPropertyValues("payment.failure-rate=" + rate)
            .run(result -> assertThat(result).hasFailed());
    }

    @TestConfiguration(proxyBeanMethods = false)
    @EnableConfigurationProperties(PaymentProperties.class)
    static class PropertiesConfiguration {}
}
