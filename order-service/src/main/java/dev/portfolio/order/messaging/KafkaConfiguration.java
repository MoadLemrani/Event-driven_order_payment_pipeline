package dev.portfolio.order.messaging;

import dev.portfolio.events.OrderEvents;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@Configuration
public class KafkaConfiguration {
    public static final String DEAD_LETTERS = "order-events.order.DLT";
    public static final String REPLAY = "order-events.order.replay";

    @Bean NewTopic orderEventsTopic() {
        return TopicBuilder.name(OrderEvents.TOPIC).partitions(3).replicas(1).build();
    }
    @Bean NewTopic deadLettersTopic() {
        return TopicBuilder.name(DEAD_LETTERS).partitions(3).replicas(1).build();
    }
    @Bean NewTopic replayTopic() {
        return TopicBuilder.name(REPLAY).partitions(3).replicas(1).build();
    }
    @Bean DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafka) {
        var recoverer = new DeadLetterPublishingRecoverer(kafka,
            (record, exception) -> new TopicPartition(DEAD_LETTERS, record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        var backoff = new ExponentialBackOffWithMaxRetries(3);
        backoff.setInitialInterval(250);
        backoff.setMultiplier(2);
        backoff.setMaxInterval(1000);
        var handler = new DefaultErrorHandler((record, exception) -> {
            // Never send a failed DLT database write back to the DLT in a loop.
            // Throwing keeps its offset uncommitted for another recovery attempt.
            if (DEAD_LETTERS.equals(record.topic())) {
                throw new IllegalStateException("Dead-letter storage unavailable", exception);
            }
            recoverer.accept(record, exception);
        }, backoff);
        handler.setClassifications(java.util.Map.of(Exception.class, true), true);
        handler.setResetStateOnExceptionChange(false);
        return handler;
    }
}
