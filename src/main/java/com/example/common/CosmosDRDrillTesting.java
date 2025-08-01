package com.example.common;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.models.CosmosClientTelemetryConfig;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.identity.DefaultAzureCredentialBuilder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CosmosDRDrillTesting {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDRDrillTesting.class.getName());

    private static final String query = "select * from c where c.id=@id and c.pk=@pk";

    private static List<CosmosAsyncClient> cosmosAsyncClients = new ArrayList<>();
    private static List<CosmosAsyncDatabase> cosmosAsyncDatabases = new ArrayList<>();
    private static List<CosmosAsyncContainer> cosmosAsyncContainers = new ArrayList<>();

    private static final CosmosEndToEndOperationLatencyPolicyConfig SIX_SECOND_E2E_TIMEOUT = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(6)).build();

    private static final CosmosItemRequestOptions POINT_REQ_OPTS = new CosmosItemRequestOptions()
            .setCosmosEndToEndOperationLatencyPolicyConfig(SIX_SECOND_E2E_TIMEOUT);

    private static final CosmosQueryRequestOptions QUERY_REQ_OPTS = new CosmosQueryRequestOptions()
            .setCosmosEndToEndOperationLatencyPolicyConfig(SIX_SECOND_E2E_TIMEOUT);

    private static final int PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();

    private static final CosmosClientTelemetryConfig TELEMETRY_CONFIG = new CosmosClientTelemetryConfig()
            .diagnosticsHandler(new SamplingCosmosDiagnosticsLogger(10, 60_000));

    private static final TokenCredential CREDENTIAL = new DefaultAzureCredentialBuilder()
            .managedIdentityClientId(Configurations.AAD_MANAGED_IDENTITY_ID)
            .authorityHost(Configurations.AAD_LOGIN_ENDPOINT)
            .tenantId(Configurations.AAD_TENANT_ID)
            .build();

    public static void main(String[] args) {

        Object waitObject = new Object();

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
                .endpoint(Configurations.endpoint)
                .preferredRegions(Configurations.PREFERRED_REGIONS);

        if (Configurations.CONNECTION_MODE_AS_STRING.equals("DIRECT")) {
            logger.info("Creating client in direct mode");
            cosmosClientBuilder = cosmosClientBuilder.directMode();
        } else if (Configurations.CONNECTION_MODE_AS_STRING.equals("GATEWAY")) {
            logger.info("Creating client in gateway mode");
            GatewayConnectionConfig gatewayConnectionConfig = GatewayConnectionConfig.getDefaultConfig();
            cosmosClientBuilder = cosmosClientBuilder.gatewayMode(gatewayConnectionConfig);
        } else {
            logger.error("Invalid connection mode: {}", Configurations.CONNECTION_MODE_AS_STRING);
            return;
        }

        if (Configurations.IS_MANAGED_IDENTITY_ENABLED) {
            logger.info("Using managed identity based authentication");
            cosmosClientBuilder = cosmosClientBuilder.credential(CREDENTIAL);
        } else {
            logger.info("Using key-based authentication");
            cosmosClientBuilder = cosmosClientBuilder.key(Configurations.key);
        }

        for (int i = 0; i < Configurations.COSMOS_CLIENT_COUNT; i++) {

            logger.info("Creating client {}", i);

            CosmosAsyncClient cosmosAsyncClient = cosmosClientBuilder
                    .userAgentSuffix("client-" + i)
                    .clientTelemetryConfig(TELEMETRY_CONFIG.enableTransportLevelTracing())
                    .buildAsyncClient();

            cosmosAsyncClients.add(cosmosAsyncClient);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //  Create initial database and container if they don't exist
        setupDBAndContainer();

        //  Insert initial data for the workload
        insertData(cosmosAsyncContainers.get(0));

        //  Start the workload
        startWorkload();

        synchronized (waitObject) {
            try {
                waitObject.wait();
            } catch (InterruptedException e) {
                logger.warn("Main thread interrupted: {}", e.getMessage(), e);
                try {
                    throw e;
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            } catch (IllegalMonitorStateException e) {
                logger.error("Illegal monitor state: {}", e.getMessage(), e);
                throw e;
            }
        }
    }

    private static void startWorkload() {
        Mono.just(1)
                .repeat()
                .flatMap(integer -> {
                    int random = ThreadLocalRandom.current().nextInt(3);
                    int containerId = ThreadLocalRandom.current().nextInt(Configurations.COSMOS_CLIENT_COUNT);
                    switch (random) {
                        case 0:
                            return Configurations.QPS > 0
                                    ? Mono.delay(Duration.ofMillis(1000 / Configurations.QPS))
                                    .then(upsertItem(cosmosAsyncContainers.get(containerId)))
                                    : upsertItem(cosmosAsyncContainers.get(containerId));
                        case 1:
                            return Configurations.QPS > 0
                                    ? Mono.delay(Duration.ofMillis(1000 / Configurations.QPS))
                                    .then(readItem(cosmosAsyncContainers.get(containerId)))
                                    : readItem(cosmosAsyncContainers.get(containerId));
                        case 2:
                            return Configurations.QPS > 0
                                    ? Mono.delay(Duration.ofMillis(1000 / Configurations.QPS))
                                    .then(queryItem(cosmosAsyncContainers.get(containerId)))
                                    : queryItem(cosmosAsyncContainers.get(containerId));
                        default:
                            return Mono.empty();
                    }
                }, Configurations.QPS > 0 ? 1 : PROCESSOR_COUNT, Configurations.QPS > 0 ? 1 : PROCESSOR_COUNT)
                .onErrorResume(throwable -> {
                    logger.error("Error occurred in workload", throwable);
                    return Mono.empty();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

    private static Mono<CosmosItemResponse<Pojo>> upsertItem(CosmosAsyncContainer cosmosAsyncContainer) {
        int finalI = ThreadLocalRandom.current().nextInt(Configurations.TOTAL_NUMBER_OF_DOCUMENTS);

        Pojo item = getItem(finalI, finalI);

        logger.debug("upsert item: {}", finalI);
        return cosmosAsyncContainer.upsertItem(item, new PartitionKey(item.getPk()), POINT_REQ_OPTS)
                .onErrorResume(throwable -> {
                    logger.error("Error occurred while upserting item", throwable);

                    if (throwable instanceof CosmosException) {
                        CosmosException cosmosException = (CosmosException) throwable;
                        logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());
                    }

                    return Mono.empty();
                });

    }

    private static Mono<CosmosItemResponse<Pojo>> readItem(CosmosAsyncContainer cosmosAsyncContainer) {

        int finalI = ThreadLocalRandom.current().nextInt(Configurations.TOTAL_NUMBER_OF_DOCUMENTS);
        logger.debug("read item: {}", finalI);
        Pojo item = getItem(finalI, finalI);
        return cosmosAsyncContainer.readItem(item.getId(), new PartitionKey(item.getPk()), POINT_REQ_OPTS, Pojo.class)
                .onErrorResume(throwable -> {
                    logger.error("Error occurred while reading item", throwable);

                    if (throwable instanceof CosmosException) {
                        CosmosException cosmosException = (CosmosException) throwable;
                        logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());
                    }

                    return Mono.empty();
                });

    }

    private static Mono<List<Pojo>> queryItem(CosmosAsyncContainer cosmosAsyncContainer) {

        int finalI = ThreadLocalRandom.current().nextInt(Configurations.TOTAL_NUMBER_OF_DOCUMENTS);
        logger.debug("query item: {}", finalI);
        Pojo item = getItem(finalI, finalI);

        SqlQuerySpec querySpec = new SqlQuerySpec(query);
        querySpec.setParameters(Arrays.asList(new SqlParameter("@id", item.getId()), new SqlParameter("@pk",
                item.getPk())));
        return cosmosAsyncContainer.queryItems(querySpec, QUERY_REQ_OPTS, Pojo.class)
                .collectList()
                .onErrorResume(throwable -> {
                    logger.error("Error occurred while querying item", throwable);

                    if (throwable instanceof CosmosException) {
                        CosmosException cosmosException = (CosmosException) throwable;
                        logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());
                    }

                    return Mono.empty();
                });

    }

    private static void insertData(CosmosAsyncContainer cosmosAsyncContainer) {
        logger.info("Inserting initial data...");

        AtomicInteger successfulInserts = new AtomicInteger(0);

        while (successfulInserts.get() < Configurations.TOTAL_NUMBER_OF_DOCUMENTS) {
            int finalI = successfulInserts.get();

            Pojo item = getItem(finalI, finalI);
            cosmosAsyncContainer
                    .upsertItem(item, new PartitionKey(item.getPk()), POINT_REQ_OPTS)
                    .doOnSuccess(ignore -> successfulInserts.incrementAndGet())
                    .onErrorResume(throwable -> {
                        logger.error("Error occurred while upserting item", throwable);
                        return Mono.empty();
                    })
                    .block();

            if (finalI % 1000 == 0) {
                logger.info("Inserting initial data - inserted {} records", finalI);
            }
        }

        logger.info("Inserted initial data...");
    }

    private static Pojo getItem(int id, int pk) {
        Pojo pojo = new Pojo();
        pojo.id = "pojo-id-" + id;
        pojo.pk = "pojo-pk-" + pk;
        pojo.field = "field-value-" + UUID.randomUUID();
        return pojo;
    }

    private static void setupDBAndContainer() {

        for (CosmosAsyncClient cosmosAsyncClient : cosmosAsyncClients) {
            try {
                logger.info("Setting up database...");
                cosmosAsyncClient.createDatabaseIfNotExists(Configurations.DATABASE_ID).block();
                CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(Configurations.DATABASE_ID);
                logger.info("Setting up container...");
                cosmosAsyncDatabase.createContainerIfNotExists(Configurations.CONTAINER_ID, Configurations.PARTITION_KEY_PATH).block();
                CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(Configurations.CONTAINER_ID);

                cosmosAsyncDatabases.add(cosmosAsyncDatabase);
                cosmosAsyncContainers.add(cosmosAsyncContainer);

            } catch (Exception e) {
                logger.error("Error occurred while creating database and container", e);
            }
            logger.info("Setup complete.");
        }
    }
}
