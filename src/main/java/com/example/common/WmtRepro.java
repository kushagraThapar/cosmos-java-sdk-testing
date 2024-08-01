package com.example.common;

import com.azure.cosmos.ConnectionMode;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosRegionSwitchHint;
import com.azure.cosmos.NonIdempotentWriteRetryOptions;
import com.azure.cosmos.SessionRetryOptionsBuilder;
import com.azure.cosmos.ThresholdBasedAvailabilityStrategy;
import com.azure.cosmos.implementation.TestConfigurations;
import com.azure.cosmos.models.CosmosItemResponse;
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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class WmtRepro {

    public static void main(String[] args) {

        WmtRepro wmtRepro = new WmtRepro();
        wmtRepro.testCreate_404_1002_FirstRegionOnly_LocalPreferred_EagerAvailabilityStrategy_WithRetries();
    }

    public void testCreate_404_1002_FirstRegionOnly_LocalPreferred_EagerAvailabilityStrategy_WithRetries() {

        System.setProperty("COSMOS.SESSION_CAPTURING_TYPE", "REGION_SCOPED");
        System.setProperty("COSMOS.PK_BASED_BLOOM_FILTER_EXPECTED_INSERTION_COUNT", "5000000");
        System.setProperty("COSMOS.PK_BASED_BLOOM_FILTER_EXPECTED_FFP_RATE", "0.001");

        CosmosAsyncClient asyncClient = buildCosmosClient(
                ConsistencyLevel.SESSION,
                Arrays.asList("West US 2", "South Central US", "East US"),
                CosmosRegionSwitchHint.LOCAL_REGION_PREFERRED,
                ConnectionMode.GATEWAY,
                new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(2))
                        .availabilityStrategy(new ThresholdBasedAvailabilityStrategy())
                        .build(),
                new NonIdempotentWriteRetryOptions()
                        .setEnabled(true)
                        .setTrackingIdUsed(true));

        CosmosAsyncDatabase asyncDatabase = asyncClient.getDatabase("testDb");
        CosmosAsyncContainer asyncContainer = asyncDatabase.getContainer("testContainer");

        FaultInjectionServerErrorResult faultInjectionServerErrorResult = FaultInjectionResultBuilders
                .getResultBuilder(FaultInjectionServerErrorType.READ_SESSION_NOT_AVAILABLE)
                .build();

        FaultInjectionCondition faultInjectionCondition = new FaultInjectionConditionBuilder()
                .operationType(FaultInjectionOperationType.CREATE_ITEM)
                .connectionType(FaultInjectionConnectionType.GATEWAY)
                .region("West US 2")
                .build();

        FaultInjectionRule faultInjectionRule = new FaultInjectionRuleBuilder("read-session-not-available-rule-" + UUID.randomUUID())
                .condition(faultInjectionCondition)
                .result(faultInjectionServerErrorResult)
                .build();

        CosmosFaultInjectionHelper.configureFaultInjectionRules(asyncContainer, Arrays.asList(faultInjectionRule)).block();

        try {
            CosmosItemResponse<TestItem> response = asyncContainer.createItem(new TestItem(UUID.randomUUID().toString(), UUID.randomUUID().toString())).block();

            System.out.println("Diagnostics : " + response.getDiagnostics());
        } catch (CosmosException ex) {

            System.out.println("Diagnostics : " + ex.getDiagnostics());
        } finally {
            asyncClient.close();

            System.clearProperty("COSMOS.SESSION_CAPTURING_TYPE");
            System.clearProperty("COSMOS.PK_BASED_BLOOM_FILTER_EXPECTED_INSERTION_COUNT");
            System.clearProperty("COSMOS.PK_BASED_BLOOM_FILTER_EXPECTED_FFP_RATE");
        }
    }

    private static CosmosAsyncClient buildCosmosClient(
            ConsistencyLevel consistencyLevel,
            List<String> preferredRegions,
            CosmosRegionSwitchHint regionSwitchHint,
            ConnectionMode connectionMode,
            CosmosEndToEndOperationLatencyPolicyConfig cosmosEndToEndOperationLatencyPolicyConfig,
            NonIdempotentWriteRetryOptions nonIdempotentWriteRetryOptions) {

        CosmosClientBuilder clientBuilder = new CosmosClientBuilder()
                .endpoint(TestConfigurations.HOST)
                .key(TestConfigurations.MASTER_KEY)
                .consistencyLevel(consistencyLevel)
                .preferredRegions(preferredRegions)
                .sessionRetryOptions(new SessionRetryOptionsBuilder()
                        .regionSwitchHint(regionSwitchHint)
                        .build())
                .endToEndOperationLatencyPolicyConfig(cosmosEndToEndOperationLatencyPolicyConfig)
                .nonIdempotentWriteRetryOptions(nonIdempotentWriteRetryOptions)
                .multipleWriteRegionsEnabled(true);

        if (connectionMode == ConnectionMode.DIRECT) {
            clientBuilder.directMode();
        } else {
            clientBuilder.gatewayMode();
        }

        return clientBuilder.buildAsyncClient();
    }

}
