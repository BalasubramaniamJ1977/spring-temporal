package com.payments.common;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;

/**
 * BeanPostProcessor that wires UETRKafkaRecordInterceptor into every
 * ConcurrentKafkaListenerContainerFactory discovered in the context.
 *
 * Because it is conditional on ConcurrentKafkaListenerContainerFactory being
 * present (activated only when spring-kafka is on the classpath), HTTP-only
 * services are completely unaffected.
 */
public class KafkaUETRInterceptorConfigurer implements BeanPostProcessor {

    private final UETRKafkaRecordInterceptor interceptor = new UETRKafkaRecordInterceptor();

    @Override
    @SuppressWarnings("unchecked")
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof ConcurrentKafkaListenerContainerFactory<?, ?> factory) {
            ((ConcurrentKafkaListenerContainerFactory<String, String>) factory)
                .setRecordInterceptor(interceptor);
        }
        return bean;
    }
}
