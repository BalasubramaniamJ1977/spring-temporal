package com.payments.processor;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean public NewTopic paymentAccepted()     { return TopicBuilder.name("payment.accepted").partitions(3).replicas(1).build(); }
    @Bean public NewTopic journalCreated()      { return TopicBuilder.name("journal.created").partitions(3).replicas(1).build(); }
    @Bean public NewTopic postingCompleted()    { return TopicBuilder.name("posting.completed").partitions(3).replicas(1).build(); }
    @Bean public NewTopic sanctionsCompleted()  { return TopicBuilder.name("sanctions.completed").partitions(3).replicas(1).build(); }
    @Bean public NewTopic fraudEventGenerated() { return TopicBuilder.name("fraud.event.generated").partitions(3).replicas(1).build(); }
    @Bean public NewTopic settlementCompleted() { return TopicBuilder.name("settlement.completed").partitions(3).replicas(1).build(); }
}
