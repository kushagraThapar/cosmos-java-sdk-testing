package com.example.common;

import com.azure.core.credential.TokenCredential;
import com.azure.core.http.ProxyOptions;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.cosmos.implementation.guava25.base.Strings;
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

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class CosmosDRDrillTesting {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDRDrillTesting.class.getName());

    private static final String DATABASE_ID = System.getProperty("DATABASE_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("DATABASE_ID")), "MigrationDB"));
    private static final String CONTAINER_ID = System.getProperty("CONTAINER_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("CONTAINER_ID")), "MigrationContainer"));
    private static final String PARTITION_KEY_PATH = "/id";
    private static final String TOTAL_DOCUMENTS = System.getProperty("TOTAL_DOCUMENTS",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("TOTAL_DOCUMENTS")), "100000"));
    private static final String CONNECTION_MODE_AS_STRING = System.getProperty("CONNECTION_MODE",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("CONNECTION_MODE")), "DIRECT")).toUpperCase(Locale.ROOT);
    private static final int TOTAL_NUMBER_OF_DOCUMENTS = Integer.parseInt(TOTAL_DOCUMENTS);

    private static final boolean IS_MANAGED_IDENTITY_ENABLED = Boolean.parseBoolean(
        System.getProperty("IS_MANAGED_IDENTITY_ENABLED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_MANAGED_IDENTITY_ENABLED")), "false")));

    private static final String AAD_LOGIN_ENDPOINT = System.getProperty("AAD_LOGIN_ENDPOINT",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("AAD_LOGIN_ENDPOINT")), "https://login.microsoftonline.com/"));

    private static final String AAD_MANAGED_IDENTITY_ID = System.getProperty("AAD_MANAGED_IDENTITY_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("AAD_MANAGED_IDENTITY_ID")), ""));

    private static final String AAD_TENANT_ID = System.getProperty("AAD_TENANT_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("AAD_TENANT_ID")), ""));

    private static final boolean IS_PROXY_ENABLED = Boolean.parseBoolean(
        System.getProperty("IS_PROXY_ENABLED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_PROXY_ENABLED")), "false")));

    private static final String PROXY_HOST = System.getProperty("PROXY_HOST",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("PROXY_HOST")), "0.0.0.0"));

    private static final int PROXY_PORT = Integer.parseInt(System.getProperty("PROXY_PORT",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("PROXY_PORT")), "5100")));

    private static final String USER_AGENT_SUFFIX = System.getProperty("USER_AGENT_SUFFIX",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("USER_AGENT_SUFFIX")), ""));

    private static final TokenCredential CREDENTIAL = new DefaultAzureCredentialBuilder()
            .managedIdentityClientId(AAD_MANAGED_IDENTITY_ID)
            .authorityHost(AAD_LOGIN_ENDPOINT)
            .tenantId(AAD_TENANT_ID)
            .build();

    private static final int COSMOS_CLIENT_COUNT = Integer.parseInt(System.getProperty("COSMOS_CLIENT_COUNT",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("COSMOS_CLIENT_COUNT")), "1")));

    private static final List<String> PREFERRED_REGIONS = Arrays.asList(
            System.getProperty("PREFERRED_REGIONS",
                StringUtils.defaultString(Strings.emptyToNull(
                    System.getenv().get("PREFERRED_REGIONS")), "East US 2 EUAP,Central US EUAP")).split(","));

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

    public static void main(String[] args) {

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
                .endpoint(Configurations.endpoint)
                .preferredRegions(PREFERRED_REGIONS);

        if (CONNECTION_MODE_AS_STRING.equals("DIRECT")) {
            logger.info("Creating client in direct mode");
            cosmosClientBuilder = cosmosClientBuilder.directMode();
        } else if (CONNECTION_MODE_AS_STRING.equals("GATEWAY")) {
            logger.info("Creating client in gateway mode");
            GatewayConnectionConfig gatewayConnectionConfig = getGatewayConnectionConfig();
            cosmosClientBuilder = cosmosClientBuilder.gatewayMode(gatewayConnectionConfig);
        } else {
            logger.error("Invalid connection mode: {}", CONNECTION_MODE_AS_STRING);
            return;
        }

        if (IS_MANAGED_IDENTITY_ENABLED) {
            logger.info("Using managed identity based authentication");
            cosmosClientBuilder = cosmosClientBuilder.credential(CREDENTIAL);
        } else {
            logger.info("Using key-based authentication");
            cosmosClientBuilder = cosmosClientBuilder.key(Configurations.key);
        }

        for (int i = 0; i < COSMOS_CLIENT_COUNT; i++) {

            logger.info("Creating client {}", i);

            CosmosAsyncClient cosmosAsyncClient = cosmosClientBuilder
                    .userAgentSuffix("client-" + i)
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

        while (true) {}
    }

    private static void startWorkload() {
        Mono.just(1)
                .repeat()
                .flatMap(integer -> {
                    int random = ThreadLocalRandom.current().nextInt(3);
                    int containerId = ThreadLocalRandom.current().nextInt(COSMOS_CLIENT_COUNT);
                    switch (random) {
                        case 0:
                            return upsertItem(cosmosAsyncContainers.get(containerId));
                        case 1:
                            return readItem(cosmosAsyncContainers.get(containerId));
                        case 2:
                            return queryItem(cosmosAsyncContainers.get(containerId));
                        default:
                            return Mono.empty();
                    }
                }, PROCESSOR_COUNT, PROCESSOR_COUNT)
                .onErrorResume(throwable -> {
                    logger.error("Error occurred in workload", throwable);
                    return Mono.empty();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
    }

    private static Mono<CosmosItemResponse<Pojo>> upsertItem(CosmosAsyncContainer cosmosAsyncContainer) {
        int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);

        Pojo item = getItem(finalI, finalI);

        logger.info("upsert item: {}", finalI);
        return cosmosAsyncContainer.upsertItem(item, new PartitionKey(item.getId()), POINT_REQ_OPTS)
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

        int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);
        logger.info("read item: {}", finalI);
        Pojo item = getItem(finalI, finalI);
        return cosmosAsyncContainer.readItem(item.getId(), new PartitionKey(item.getId()), POINT_REQ_OPTS, Pojo.class)
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

        int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);
        logger.info("query item: {}", finalI);
        Pojo item = getItem(finalI, finalI);

        SqlQuerySpec querySpec = new SqlQuerySpec(query);
        querySpec.setParameters(Arrays.asList(new SqlParameter("@id", item.getId()), new SqlParameter("@pk",
                item.getId())));
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

        while (successfulInserts.get() < TOTAL_NUMBER_OF_DOCUMENTS) {
            int finalI = successfulInserts.get();

            Pojo item = getItem(finalI, finalI);
            cosmosAsyncContainer
                    .upsertItem(item, new PartitionKey(item.getId()), POINT_REQ_OPTS)
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
                cosmosAsyncClient.createDatabaseIfNotExists(DATABASE_ID).block();
                CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(DATABASE_ID);
                logger.info("Setting up container...");
                cosmosAsyncDatabase.createContainerIfNotExists(CONTAINER_ID, PARTITION_KEY_PATH).block();
                CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(CONTAINER_ID);

                cosmosAsyncDatabases.add(cosmosAsyncDatabase);
                cosmosAsyncContainers.add(cosmosAsyncContainer);

            } catch (Exception e) {
                logger.error("Error occurred while creating database and container", e);
            }
            logger.info("Setup complete.");
        }
    }

    private static GatewayConnectionConfig getGatewayConnectionConfig() {
        GatewayConnectionConfig gatewayConnectionConfig = GatewayConnectionConfig.getDefaultConfig();

        if (IS_PROXY_ENABLED) {
            System.setProperty("COSMOS.EMULATOR_SERVER_CERTIFICATE_VALIDATION_DISABLED", "true");
            gatewayConnectionConfig.setProxy(new ProxyOptions(ProxyOptions.Type.HTTP, InetSocketAddress.createUnresolved(PROXY_HOST, PROXY_PORT)));
        }

        return gatewayConnectionConfig;
    }
}
