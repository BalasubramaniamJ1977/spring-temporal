package com.payments.temporal.config;

import com.payments.temporal.activity.PaymentActivitiesImpl;
import com.payments.temporal.activity.ComplianceActivitiesImpl;
import com.payments.temporal.workflow.PaymentWorkflowImpl;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowClientOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class TemporalConfig {

    @Value("${temporal.address:temporal:7233}")
    private String temporalAddress;

    @Value("${temporal.namespace:default}")
    private String namespace;

    @Value("${temporal.task-queue:payment-processing}")
    private String taskQueue;

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

    @Bean(destroyMethod = "shutdown")
    public WorkerFactory workerFactory(WorkflowClient client,
                                       PaymentActivitiesImpl paymentActivities,
                                       ComplianceActivitiesImpl complianceActivities) {
        WorkerFactory factory = WorkerFactory.newInstance(client);
        Worker worker = factory.newWorker(taskQueue);
        worker.registerWorkflowImplementationTypes(PaymentWorkflowImpl.class);
        worker.registerActivitiesImplementations(paymentActivities, complianceActivities);
        factory.start();

        log.info("Temporal worker registered on task queue '{}' at {}", taskQueue, temporalAddress);
        return factory;
    }
}
