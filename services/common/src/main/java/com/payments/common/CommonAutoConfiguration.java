package com.payments.common;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

/**
 * Auto-configuration loaded via
 * META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports.
 *
 * Every service that depends on the common module gets for free:
 *   - UETRTraceFilter   (payment.uetr → span attribute + MDC)
 *   - RestTemplate bean (instrumented by the OTEL Java agent for trace propagation)
 *   - Error forwarder   (propagates downstream 4xx/5xx status codes upstream)
 */
@AutoConfiguration
public class CommonAutoConfiguration {

    @Bean
    public FilterRegistrationBean<UETRTraceFilter> uetrTraceFilter() {
        FilterRegistrationBean<UETRTraceFilter> reg = new FilterRegistrationBean<>(new UETRTraceFilter());
        reg.addUrlPatterns("/*");
        reg.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return reg;
    }

    @Bean
    @ConditionalOnMissingBean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        // The OTEL Java agent instruments RestTemplate to inject
        // traceparent / tracestate into every outbound request automatically.
        return builder.build();
    }

    /**
     * Applies UETRKafkaRecordInterceptor to every Kafka listener container factory.
     * Conditional on ConcurrentKafkaListenerContainerFactory — no-op for HTTP-only services.
     */
    @Bean
    @ConditionalOnClass(ConcurrentKafkaListenerContainerFactory.class)
    public KafkaUETRInterceptorConfigurer kafkaUETRInterceptorConfigurer() {
        return new KafkaUETRInterceptorConfigurer();
    }

    /** Propagate downstream HTTP errors back to the caller with the original status code. */
    @RestControllerAdvice
    static class DownstreamErrorForwarder {
        @ExceptionHandler(HttpStatusCodeException.class)
        ResponseEntity<String> forward(HttpStatusCodeException ex) {
            return ResponseEntity.status(ex.getStatusCode())
                                 .body(ex.getResponseBodyAsString());
        }
    }
}
