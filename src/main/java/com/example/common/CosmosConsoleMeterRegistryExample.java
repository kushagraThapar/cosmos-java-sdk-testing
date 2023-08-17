package com.example.common;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosRegionSwitchHint;
import com.azure.cosmos.SessionRetryOptionsBuilder;
import com.azure.cosmos.implementation.HttpConstants;
import com.azure.cosmos.implementation.Utils;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosMetricCategory;
import com.azure.cosmos.models.CosmosMetricTagName;
import com.azure.cosmos.models.CosmosMicrometerMetricsOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.test.faultinjection.CosmosFaultInjectionHelper;
import com.azure.cosmos.test.faultinjection.FaultInjectionCondition;
import com.azure.cosmos.test.faultinjection.FaultInjectionConditionBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionConnectionType;
import com.azure.cosmos.test.faultinjection.FaultInjectionOperationType;
import com.azure.cosmos.test.faultinjection.FaultInjectionResultBuilders;
import com.azure.cosmos.test.faultinjection.FaultInjectionRule;
import com.azure.cosmos.test.faultinjection.FaultInjectionRuleBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorResult;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.microsoft.applicationinsights.core.dependencies.apachecommons.lang3.tuple.ImmutablePair;
import com.microsoft.applicationinsights.core.dependencies.apachecommons.lang3.tuple.Pair;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CosmosConsoleMeterRegistryExample {

    private static final Logger logger = LoggerFactory.getLogger(CosmosConsoleMeterRegistryExample.class);

    public static void main(String[] args) {
        createReadSessionNotAvailableThroughFaultInjection();
        createReadSessionNotAvailableThroughInvalidSessionToken();
    }

    public static void createReadSessionNotAvailableThroughFaultInjection() {
        System.setProperty("COSMOS.MAX_RETRIES_IN_LOCAL_REGION_WHEN_REMOTE_REGION_PREFERRED", String.valueOf(2));

        MeterRegistry meterRegistry = createMeterRegistry();
        CosmosMicrometerMetricsOptions inputMetricOptions = createMicrometerMetricsOptions(meterRegistry);
        CosmosClientTelemetryConfig clientTelemetryConfig = createClientTelemetryConfig(inputMetricOptions);

        CosmosAsyncClient clientWithPreferredRegions = null;

        try {
            clientWithPreferredRegions = buildCosmosClient(clientTelemetryConfig);

            // setup db and container and pass their ids accordingly
            // ensure the container has a partition key definition of /mypk
            CosmosAsyncContainer containerForClientWithPreferredRegions = clientWithPreferredRegions
                .getDatabase("test-db")
                .getContainer("test-container");

            String documentId = UUID.randomUUID().toString();
            Pair<String, String> idAndPkValPair = new ImmutablePair<>(documentId, documentId);

            TestItem createdItem = new TestItem(documentId, documentId);
            containerForClientWithPreferredRegions.createItem(createdItem).block();

            FaultInjectionRuleBuilder badSessionTokenRuleBuilder = new FaultInjectionRuleBuilder("serverErrorRule-bad"
                + "-session-token-" + UUID.randomUUID());

            // inject 404/1002s in two regions
            // configure in accordance with preferredRegions on the client
            FaultInjectionCondition faultInjectionConditionForReadsInPrimaryRegion =
                new FaultInjectionConditionBuilder()
                    .operationType(FaultInjectionOperationType.READ_ITEM)
                    .connectionType(FaultInjectionConnectionType.DIRECT)
                    .region("East US")
                    .build();

            FaultInjectionCondition faultInjectionConditionForReadsInSecondaryRegion =
                new FaultInjectionConditionBuilder()
                    .operationType(FaultInjectionOperationType.READ_ITEM)
                    .connectionType(FaultInjectionConnectionType.DIRECT)
                    .region("West US")
                    .build();

            FaultInjectionServerErrorResult badSessionTokenServerErrorResult = FaultInjectionResultBuilders
                .getResultBuilder(FaultInjectionServerErrorType.READ_SESSION_NOT_AVAILABLE)
                .build();

            // sustained fault injection
            FaultInjectionRule readSessionUnavailableRulePrimaryRegion = badSessionTokenRuleBuilder
                .condition(faultInjectionConditionForReadsInPrimaryRegion)
                .result(badSessionTokenServerErrorResult)
                .duration(Duration.ofSeconds(30))
                .build();

            // sustained fault injection
            FaultInjectionRule readSessionUnavailableRuleSecondaryRegion = badSessionTokenRuleBuilder
                .condition(faultInjectionConditionForReadsInSecondaryRegion)
                .result(badSessionTokenServerErrorResult)
                .duration(Duration.ofSeconds(30))
                .build();

            CosmosFaultInjectionHelper
                .configureFaultInjectionRules(containerForClientWithPreferredRegions,
                    Arrays.asList(readSessionUnavailableRulePrimaryRegion, readSessionUnavailableRuleSecondaryRegion))
                .block();

            int threadCount = 1;

            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            AtomicBoolean isStopped = new AtomicBoolean(false);

            Duration workloadExecutionDuration = Duration.ofSeconds(30);

            Flux
                .just(1)
                .delayElements(workloadExecutionDuration)
                .doOnComplete(() -> isStopped.compareAndSet(false, true))
                .subscribe();

            CosmosEndToEndOperationLatencyPolicyConfig endToEndOperationLatencyPolicyConfig =
                new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(2)).build();

            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> execute(
                    containerForClientWithPreferredRegions,
                    idAndPkValPair,
                    isStopped,
                    endToEndOperationLatencyPolicyConfig,
                    null)
                );
            }

            try {
                executorService.awaitTermination(30, TimeUnit.SECONDS);
                executorService.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        } catch (Exception e) {
            logger.error("Error occurred", e);
        } finally {
            System.clearProperty("COSMOS.MAX_RETRIES_IN_LOCAL_REGION_WHEN_REMOTE_REGION_PREFERRED");
            clientWithPreferredRegions.close();
        }
    }

    public static void createReadSessionNotAvailableThroughInvalidSessionToken() {
        System.setProperty("COSMOS.MAX_RETRIES_IN_LOCAL_REGION_WHEN_REMOTE_REGION_PREFERRED", String.valueOf(2));

        MeterRegistry meterRegistry = createMeterRegistry();
        CosmosMicrometerMetricsOptions inputMetricOptions = createMicrometerMetricsOptions(meterRegistry);
        CosmosClientTelemetryConfig clientTelemetryConfig = createClientTelemetryConfig(inputMetricOptions);

        CosmosAsyncClient clientWithPreferredRegions = null;

        try {
            clientWithPreferredRegions = buildCosmosClient(clientTelemetryConfig);

            // setup db and container and pass their ids accordingly
            // ensure the container has a partition key definition of /mypk
            CosmosAsyncContainer containerForClientWithPreferredRegions = clientWithPreferredRegions
                .getDatabase("test-db")
                .getContainer("test-container");

            String documentId = UUID.randomUUID().toString();
            Pair<String, String> idAndPkValPair = new ImmutablePair<>(documentId, documentId);

            TestItem createdItem = new TestItem(documentId, documentId);
            containerForClientWithPreferredRegions.createItem(createdItem).block();

            FaultInjectionRuleBuilder badSessionTokenRuleBuilder = new FaultInjectionRuleBuilder("serverErrorRule-bad"
                + "-session-token-" + UUID.randomUUID());

            // inject 404/1002s in two regions
            // configure regions accordingly
            FaultInjectionCondition faultInjectionConditionForReadsInPrimaryRegion =
                new FaultInjectionConditionBuilder()
                    .operationType(FaultInjectionOperationType.READ_ITEM)
                    .connectionType(FaultInjectionConnectionType.DIRECT)
                    .region("East US")
                    .build();

            FaultInjectionCondition faultInjectionConditionForReadsInSecondaryRegion =
                new FaultInjectionConditionBuilder()
                    .operationType(FaultInjectionOperationType.READ_ITEM)
                    .connectionType(FaultInjectionConnectionType.DIRECT)
                    .region("West US")
                    .build();

            FaultInjectionServerErrorResult badSessionTokenServerErrorResult = FaultInjectionResultBuilders
                .getResultBuilder(FaultInjectionServerErrorType.READ_SESSION_NOT_AVAILABLE)
                .build();

            // sustained fault injection
            FaultInjectionRule readSessionUnavailableRulePrimaryRegion = badSessionTokenRuleBuilder
                .condition(faultInjectionConditionForReadsInPrimaryRegion)
                .result(badSessionTokenServerErrorResult)
                .duration(Duration.ofSeconds(30))
                .build();

            // sustained fault injection
            FaultInjectionRule readSessionUnavailableRuleSecondaryRegion = badSessionTokenRuleBuilder
                .condition(faultInjectionConditionForReadsInSecondaryRegion)
                .result(badSessionTokenServerErrorResult)
                .duration(Duration.ofSeconds(30))
                .build();

            CosmosFaultInjectionHelper
                .configureFaultInjectionRules(containerForClientWithPreferredRegions,
                    Arrays.asList(readSessionUnavailableRulePrimaryRegion, readSessionUnavailableRuleSecondaryRegion))
                .block();

            int threadCount = 1;

            ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
            AtomicBoolean isStopped = new AtomicBoolean(false);

            Duration workloadExecutionDuration = Duration.ofSeconds(30);

            Flux
                .just(1)
                .delayElements(workloadExecutionDuration)
                .doOnComplete(() -> isStopped.compareAndSet(false, true))
                .subscribe();

            // configure / disable endToEnd timeout accordingly
            CosmosEndToEndOperationLatencyPolicyConfig endToEndOperationLatencyPolicyConfig =
                new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(2)).build();

            // pass invalid session token accordingly
            String invalidSessionToken = "0:0#909#7=10000";

            for (int i = 0; i < threadCount; i++) {
                executorService.submit(() -> execute(
                    containerForClientWithPreferredRegions,
                    idAndPkValPair,
                    isStopped,
                    endToEndOperationLatencyPolicyConfig,
                    invalidSessionToken)
                );
            }

            try {
                executorService.awaitTermination(30, TimeUnit.SECONDS);
                executorService.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        } catch (Exception e) {
            logger.error("Error occurred", e);
        } finally {

            System.clearProperty("COSMOS.MAX_RETRIES_IN_LOCAL_REGION_WHEN_REMOTE_REGION_PREFERRED");
            clientWithPreferredRegions.close();
        }
    }

    private static void execute(
        CosmosAsyncContainer container,
        Pair<String, String> idAndPkValPair,
        AtomicBoolean isStopped,
        CosmosEndToEndOperationLatencyPolicyConfig endToEndOperationLatencyPolicyConfig,
        String invalidSessionToken) {

        while (!isStopped.get()) {
            try {
                CosmosItemRequestOptions itemRequestOptions = new CosmosItemRequestOptions();

                if (endToEndOperationLatencyPolicyConfig != null) {
                    itemRequestOptions.setCosmosEndToEndOperationLatencyPolicyConfig(endToEndOperationLatencyPolicyConfig);
                }

                if (invalidSessionToken != null && !invalidSessionToken.isEmpty()) {
                    itemRequestOptions.setSessionToken(invalidSessionToken);
                }

                CosmosItemResponse<ObjectNode> response = container.readItem(idAndPkValPair.getLeft(),
                    new PartitionKey(idAndPkValPair.getRight()), itemRequestOptions, ObjectNode.class).block();
            } catch (Exception e) {
                // add more logging
                if (e instanceof CosmosException) {
                    CosmosException cosmosException = Utils.as(e, CosmosException.class);
                } else {

                }
            }
        }
    }

    private static CosmosClientTelemetryConfig createClientTelemetryConfig(CosmosMicrometerMetricsOptions inputMetricsOptions) {
        return new CosmosClientTelemetryConfig()
            .metricsOptions(inputMetricsOptions);
    }

    private static CosmosAsyncClient buildCosmosClient(CosmosClientTelemetryConfig clientTelemetryConfig) {

        // configure preferred regions accordingly
        List<String> preferredRegions = Arrays.asList("East US", "West US");

        return new CosmosClientBuilder()
            .endpoint(Configurations.endpoint)
            .key(Configurations.key)
            .consistencyLevel(ConsistencyLevel.SESSION)
            .preferredRegions(preferredRegions)
            .sessionRetryOptions(new SessionRetryOptionsBuilder()
                .regionSwitchHint(CosmosRegionSwitchHint.LOCAL_REGION_PREFERRED)
                .build())
            .directMode()
            .multipleWriteRegionsEnabled(true)
            .clientTelemetryConfig(clientTelemetryConfig)
            .buildAsyncClient();
    }

    private static MeterRegistry createMeterRegistry() {
        return ConsoleLoggingRegistryFactory.create(1);
    }

    private static CosmosMicrometerMetricsOptions createMicrometerMetricsOptions(MeterRegistry meterRegistry) {
        return new CosmosMicrometerMetricsOptions()
            .setEnabled(true)
            .meterRegistry(meterRegistry)
            .addMetricCategories(CosmosMetricCategory.OPERATION_DETAILS)
            .addMetricCategories(CosmosMetricCategory.REQUEST_DETAILS)
            .enableHistogramsByDefault(false)
            .configureDefaultPercentiles(0.99)
            .configureDefaultTagNames(CosmosMetricTagName.SERVICE_ADDRESS, CosmosMetricTagName.REGION_NAME);
    }
}
