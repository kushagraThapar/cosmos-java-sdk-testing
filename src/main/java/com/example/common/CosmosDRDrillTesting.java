package com.example.common;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.cosmos.implementation.guava25.base.Strings;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.identity.DefaultAzureCredentialBuilder;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CosmosDRDrillTesting {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDRDrillTesting.class.getName());

    private static final String DATABASE_ID = System.getProperty("DATABASE_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("DATABASE_ID")), "MigrationDB"));
    private static final String CONTAINER_ID = System.getProperty("CONTAINER_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("CONTAINER_ID")), "MigrationContainer"));
    private static final String PARTITION_KEY_PATH = "/pk";
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

    private static final TokenCredential CREDENTIAL = new DefaultAzureCredentialBuilder()
            .managedIdentityClientId(AAD_MANAGED_IDENTITY_ID)
            .authorityHost(AAD_LOGIN_ENDPOINT)
            .tenantId(AAD_TENANT_ID)
            .build();

    private static CosmosAsyncClient cosmosAsyncClient;
    private static CosmosAsyncDatabase cosmosAsyncDatabase;
    private static CosmosAsyncContainer cosmosAsyncContainer;

    private static final CosmosEndToEndOperationLatencyPolicyConfig SIX_SECOND_E2E_TIMEOUT = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(6)).build();

    private static final CosmosItemRequestOptions POINT_REQ_OPTS = new CosmosItemRequestOptions()
            .setCosmosEndToEndOperationLatencyPolicyConfig(SIX_SECOND_E2E_TIMEOUT);

    private static final CosmosQueryRequestOptions QUERY_REQ_OPTS = new CosmosQueryRequestOptions()
            .setCosmosEndToEndOperationLatencyPolicyConfig(SIX_SECOND_E2E_TIMEOUT);

    public static void main(String[] args) {

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
                .contentResponseOnWriteEnabled(true)
                .endpoint(Configurations.endpoint);

        ScheduledThreadPoolExecutor scheduledExecutor =
                new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());

        if (CONNECTION_MODE_AS_STRING.equals("DIRECT")) {
            logger.info("Creating client in direct mode");
            cosmosClientBuilder = cosmosClientBuilder.directMode();
        } else if (CONNECTION_MODE_AS_STRING.equals("GATEWAY")) {
            logger.info("Creating client in gateway mode");
            cosmosClientBuilder = cosmosClientBuilder.gatewayMode();
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

        cosmosAsyncClient = cosmosClientBuilder
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();

        //  Create initial database and container if they don't exist
        setupDBAndContainer();

        //  Insert initial data for the workload
        insertData();

        //  Start the workload
        startWorkload(scheduledExecutor);
    }

    private static void startWorkload(ScheduledThreadPoolExecutor scheduledExecutor) {
        while (true) {
            int randomOperation = ThreadLocalRandom.current().nextInt(4);
            switch (randomOperation) {
                case 1:
                    upsertItem(scheduledExecutor);
                    break;
                case 2:
                    readItem(scheduledExecutor);
                    break;
                case 3:
                    queryItem(scheduledExecutor);
                    break;
                default:
            }
        }
    }

    private static void upsertItem(ScheduledThreadPoolExecutor scheduledExecutor) {
        try {
            int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);

            Pojo item = getItem(finalI, finalI);

            logger.info("upsert item: {}", finalI);
            scheduledExecutor.schedule(() -> cosmosAsyncContainer.upsertItem(item, new PartitionKey(item.getPk()), POINT_REQ_OPTS)
                    .onErrorResume(throwable -> {
                        logger.error("Error occurred while upserting item", throwable);

                        if (throwable instanceof CosmosException) {
                            CosmosException cosmosException = (CosmosException) throwable;
                            logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());
                        }

                        return Mono.empty();
                    })
                    .block(), 10, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error occurred while upserting item", e);
        }
    }

    private static void readItem(ScheduledThreadPoolExecutor scheduledExecutor) {
        try {
            int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);
            logger.info("read item: {}", finalI);
            Pojo item = getItem(finalI, finalI);
            scheduledExecutor.schedule(() ->
                    cosmosAsyncContainer.readItem(item.getId(), new PartitionKey(item.getPk()), POINT_REQ_OPTS, Pojo.class)
                            .onErrorResume(throwable -> {
                                logger.error("Error occurred while reading item", throwable);

                                if (throwable instanceof CosmosException) {
                                    CosmosException cosmosException = (CosmosException) throwable;
                                    logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());
                                }

                                return Mono.empty();
                            })
                            .block(), 10, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error occurred while reading item", e);
        }
    }

    private static void queryItem(ScheduledThreadPoolExecutor scheduledExecutor) {
        try {
            int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);
            logger.info("query item: {}", finalI);
            Pojo item = getItem(finalI, finalI);
            String query = "select * from c where c.id=@id and c.pk=@pk";
            SqlQuerySpec querySpec = new SqlQuerySpec(query);
            querySpec.setParameters(Arrays.asList(new SqlParameter("@id", item.getId()), new SqlParameter("@pk",
                item.getPk())));
            scheduledExecutor.schedule(() -> cosmosAsyncContainer.queryItems(querySpec, QUERY_REQ_OPTS, Pojo.class)
                    .collectList()
                    .onErrorResume(throwable -> {
                        logger.error("Error occurred while querying item", throwable);

                        if (throwable instanceof CosmosException) {
                            CosmosException cosmosException = (CosmosException) throwable;
                            logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());
                        }

                        return Mono.empty();
                    })
                    .block(), 10, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            logger.error("Error occurred while querying item", e);
        }
    }

    private static void insertData() {
        logger.info("Inserting initial data...");

        AtomicInteger successfulInserts = new AtomicInteger(0);

        while (successfulInserts.get() < TOTAL_NUMBER_OF_DOCUMENTS) {
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
        try {
            logger.info("Setting up database...");
            cosmosAsyncClient.createDatabaseIfNotExists(DATABASE_ID).block();
            cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(DATABASE_ID);
            logger.info("Setting up container...");
            cosmosAsyncDatabase.createContainerIfNotExists(CONTAINER_ID, PARTITION_KEY_PATH).block();
            cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(CONTAINER_ID);
        } catch (Exception e) {
            logger.error("Error occurred while creating database and container", e);
        }
        logger.info("Setup complete.");
    }
}
