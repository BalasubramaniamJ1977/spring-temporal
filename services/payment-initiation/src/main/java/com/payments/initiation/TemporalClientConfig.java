package com.payments.initiation;

import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TemporalClientConfig {

    @Value("${temporal.address:temporal:7233}")
    private String temporalAddress;

    @Value("${temporal.namespace:default}")
    private String namespace;

    @Bean(destroyMethod = "shutdown")
    public WorkflowServiceStubs workflowServiceStubs() {
        return WorkflowServiceStubs.newServiceStubs(
            WorkflowServiceStubsOptions.newBuilder()
                .setTarget(temporalAddress)
                .build());
    }

    @Bean
    public WorkflowClient workflowClient(WorkflowServiceStubs stubs) {
        return WorkflowClient.newInstance(stubs,
            WorkflowClientOptions.newBuilder()
                .setNamespace(namespace)
                .build());
    }
}
