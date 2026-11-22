package dev.portfolio.order.observability;

import dev.portfolio.order.messaging.KafkaConfiguration;
import dev.portfolio.events.OrderEvents;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ConsumerLagMetrics {
    private final AdminClient admin;
    private volatile double businessLag = Double.NaN;
    private volatile double deadLetterLag = Double.NaN;
    private volatile double lastSuccess;

    public ConsumerLagMetrics(@Value("${spring.kafka.bootstrap-servers}") String brokers, MeterRegistry registry) {
        admin = AdminClient.create(Map.of("bootstrap.servers", brokers,
            "request.timeout.ms", 3000, "default.api.timeout.ms", 3000));
        Gauge.builder("pipeline.consumer.lag", this, m -> m.businessLag)
            .tag("group", "order-service").register(registry);
        Gauge.builder("pipeline.consumer.lag", this, m -> m.deadLetterLag)
            .tag("group", "order-dead-letter-store").register(registry);
        Gauge.builder("pipeline.consumer.lag.last.success", this, m -> m.lastSuccess)
            .baseUnit("seconds").register(registry);
    }

    @Scheduled(fixedDelayString = "${metrics.lag.poll-interval-ms:5000}")
    public void refresh() {
        try {
            double business = lag("order-service", List.of(OrderEvents.TOPIC, KafkaConfiguration.REPLAY));
            double dead = lag("order-dead-letter-store", List.of(KafkaConfiguration.DEAD_LETTERS));
            businessLag = business;
            deadLetterLag = dead;
            lastSuccess = Instant.now().getEpochSecond();
        } catch (Exception ex) {
            if (ex instanceof InterruptedException) Thread.currentThread().interrupt();
            LoggerFactory.getLogger(ConsumerLagMetrics.class).warn("Consumer lag refresh failed: {}", ex.toString());
            // Retain the last snapshot; its timestamp exposes staleness instead of reporting a false zero.
        }
    }

    private long lag(String group, List<String> topics) throws Exception {
        var descriptions = admin.describeTopics(topics).allTopicNames().get();
        var latestRequest = new HashMap<TopicPartition, OffsetSpec>();
        var earliestRequest = new HashMap<TopicPartition, OffsetSpec>();
        descriptions.forEach((topic, description) -> description.partitions().forEach(partition -> {
            var tp = new TopicPartition(topic, partition.partition());
            latestRequest.put(tp, OffsetSpec.latest());
            earliestRequest.put(tp, OffsetSpec.earliest());
        }));
        var latest = admin.listOffsets(latestRequest).all().get();
        var earliest = admin.listOffsets(earliestRequest).all().get();
        var committed = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
        long total = 0;
        for (var entry : latest.entrySet()) {
            var offset = committed.get(entry.getKey());
            long position = offset == null ? earliest.get(entry.getKey()).offset() : offset.offset();
            total += Math.max(0, entry.getValue().offset() - position);
        }
        return total;
    }

    @PreDestroy
    public void close() { admin.close(Duration.ofSeconds(1)); }
}
