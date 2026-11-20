package dev.portfolio.inventory.messaging;

import dev.portfolio.events.OrderEvents;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfiguration {
    @Bean
    NewTopic orderEventsTopic() {
        return TopicBuilder.name(OrderEvents.TOPIC).partitions(3).replicas(1).build();
    }
}
