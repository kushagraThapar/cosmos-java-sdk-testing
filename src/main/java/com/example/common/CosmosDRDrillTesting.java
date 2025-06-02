package com.example.common;

import com.azure.core.credential.TokenCredential;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfig;
import com.azure.cosmos.CosmosEndToEndOperationLatencyPolicyConfigBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ReadConsistencyStrategy;
import com.azure.cosmos.ThresholdBasedAvailabilityStrategy;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.cosmos.implementation.guava25.base.Strings;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.FeedRange;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.test.faultinjection.CosmosFaultInjectionHelper;
import com.azure.cosmos.test.faultinjection.FaultInjectionCondition;
import com.azure.cosmos.test.faultinjection.FaultInjectionConditionBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionConnectionType;
import com.azure.cosmos.test.faultinjection.FaultInjectionEndpointBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionResultBuilders;
import com.azure.cosmos.test.faultinjection.FaultInjectionRule;
import com.azure.cosmos.test.faultinjection.FaultInjectionRuleBuilder;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorResult;
import com.azure.cosmos.test.faultinjection.FaultInjectionServerErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.azure.identity.DefaultAzureCredentialBuilder;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final Integer IDLE_CONNECTION_TIMEOUT_IN_SECONDS = Integer.parseInt(
            System.getProperty("IDLE_CONNECTION_TIMEOUT_IN_SECONDS",
                    StringUtils.defaultString(Strings.emptyToNull(
                            System.getenv().get("IDLE_CONNECTION_TIMEOUT_IN_SECONDS")), "60")));

    private static final Integer DELAY_BETWEEN_OPERATIONS_IN_SECONDS = Integer.parseInt(
        System.getProperty("DELAY_BETWEEN_OPERATIONS_IN_SECONDS",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("DELAY_BETWEEN_OPERATIONS_IN_SECONDS")), "30")));

    private static final boolean IS_DELAY_BETWEEN_OPERATIONS_ENABLED = Boolean.parseBoolean(
        System.getProperty("IS_DELAY_BETWEEN_OPERATIONS_ENABLED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_DELAY_BETWEEN_OPERATIONS_ENABLED")), "false")));

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

    private static final String USER_AGENT = System.getProperty("USER_AGENT",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("USER_AGENT")), ""));

    private static final boolean IS_PARTITION_MIGRATION_FAULT_INJECTION_ENABLED = Boolean.parseBoolean(
        System.getProperty("IS_PARTITION_MIGRATION_FAULT_INJECTION_ENABLED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_PARTITION_MIGRATION_FAULT_INJECTION_ENABLED")), "false")));

    private static final boolean IS_LATEST_COMMITTED_READ_CONSISTENCY_STRATEGY_ENABLED = Boolean.parseBoolean(
        System.getProperty("IS_LATEST_COMMITTED_READ_CONSISTENCY_STRATEGY_ENABLED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_LATEST_COMMITTED_READ_CONSISTENCY_STRATEGY_ENABLED")), "false")));

    private static final boolean IS_PER_PARTITION_CIRCUIT_BREAKER_ENABLED = Boolean.parseBoolean(
        System.getProperty("IS_PER_PARTITION_CIRCUIT_BREAKER_ENABLED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_PER_PARTITION_CIRCUIT_BREAKER_ENABLED")), "false")));

    private static final boolean IS_AVAILABILITY_STRATEGY_ENABLED_FOR_READS = Boolean.parseBoolean(
        System.getProperty("IS_AVAILABILITY_STRATEGY_ENABLED_FOR_READS",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_AVAILABILITY_STRATEGY_ENABLED_FOR_READS")), "false")));

    private static final Scheduler FAULT_INJECTION_SCHEDULER = Schedulers.newSingle("fault-injector-single", true);

    private static final String query = "select * from c where c.id=@id and c.pk=@pk";

    private static CosmosAsyncClient cosmosAsyncClient;
    private static CosmosAsyncDatabase cosmosAsyncDatabase;
    private static CosmosAsyncContainer cosmosAsyncContainer;

    private static final CosmosEndToEndOperationLatencyPolicyConfig SIX_SECOND_E2E_TIMEOUT = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(6)).build();

    private static final CosmosEndToEndOperationLatencyPolicyConfig FIVE_SECOND_E2E_TIMEOUT_WITH_DEFAULT_AVAILABILITY_STRATEGY
            = new CosmosEndToEndOperationLatencyPolicyConfigBuilder(Duration.ofSeconds(5))
            .availabilityStrategy(new ThresholdBasedAvailabilityStrategy())
            .build();

    private static final CosmosItemRequestOptions POINT_REQ_OPTS = new CosmosItemRequestOptions()
            .setCosmosEndToEndOperationLatencyPolicyConfig(SIX_SECOND_E2E_TIMEOUT);

    private static final CosmosQueryRequestOptions QUERY_REQ_OPTS = new CosmosQueryRequestOptions()
            .setCosmosEndToEndOperationLatencyPolicyConfig(SIX_SECOND_E2E_TIMEOUT);

    private static final int PROCESSOR_COUNT = Runtime.getRuntime().availableProcessors();

    private static final FaultInjectionServerErrorResult FAULT_INJECTION_PARTITION_IS_MIGRATING_SERVER_ERROR_RESULT = FaultInjectionResultBuilders
            .getResultBuilder(FaultInjectionServerErrorType.PARTITION_IS_MIGRATING)
            .build();

    private static final FaultInjectionCondition FAULT_INJECTION_CONDITION = new FaultInjectionConditionBuilder()
            .connectionType(FaultInjectionConnectionType.DIRECT)
            .endpoints(new FaultInjectionEndpointBuilder(FeedRange.forFullRange()).includePrimary(false).replicaCount(2).build())
            .region("East US")
            .build();

    private static final FaultInjectionRule FAULT_INJECTION_RULE = new FaultInjectionRuleBuilder("fault-injection-rule-" + UUID.randomUUID())
            .condition(FAULT_INJECTION_CONDITION)
            .result(FAULT_INJECTION_PARTITION_IS_MIGRATING_SERVER_ERROR_RESULT)
            .duration(Duration.ofMinutes(10))
            .startDelay(Duration.ofMinutes(5))
            .build();

    private static final boolean IS_UPSERT_EXCLUDED = Boolean.parseBoolean(
        System.getProperty("IS_UPSERT_EXCLUDED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_UPSERT_EXCLUDED")), "false")));

    private static final boolean IS_READ_EXCLUDED = Boolean.parseBoolean(
        System.getProperty("IS_READ_EXCLUDED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_READ_EXCLUDED")), "false")));

    private static final boolean IS_QUERY_EXCLUDED = Boolean.parseBoolean(
        System.getProperty("IS_QUERY_EXCLUDED",
            StringUtils.defaultString(Strings.emptyToNull(
                System.getenv().get("IS_QUERY_EXCLUDED")), "false")));

    private static final List<FaultInjectionRule> FAULT_INJECTION_RULES = Arrays.asList(FAULT_INJECTION_RULE);

    private static final CosmosItemRequestOptions POINT_REQ_OPTS_WITH_AVAILABILITY_STRATEGY = new CosmosItemRequestOptions()
            .setCosmosEndToEndOperationLatencyPolicyConfig(FIVE_SECOND_E2E_TIMEOUT_WITH_DEFAULT_AVAILABILITY_STRATEGY);

    public static void main(String[] args) {

        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
                .contentResponseOnWriteEnabled(true)
                .endpoint(Configurations.endpoint)
                .dnsLookupLoggingEnabled(true)
                .userAgentSuffix(USER_AGENT);

        if (IS_LATEST_COMMITTED_READ_CONSISTENCY_STRATEGY_ENABLED) {
            logger.info("Using Local Committed Read Consistency");
            cosmosClientBuilder = cosmosClientBuilder.readConsistencyStrategy(ReadConsistencyStrategy.LATEST_COMMITTED);
        } else {
            logger.info("Using default consistency level");
        }

        if (IS_PER_PARTITION_CIRCUIT_BREAKER_ENABLED) {
            logger.info("Using per partition circuit breaker");
            System.setProperty(
                    "COSMOS.PARTITION_LEVEL_CIRCUIT_BREAKER_CONFIG",
                    "{\"isPartitionLevelCircuitBreakerEnabled\": true, "
                            + "\"circuitBreakerType\": \"CONSECUTIVE_EXCEPTION_COUNT_BASED\","
                            + "\"consecutiveExceptionCountToleratedForReads\": 10,"
                            + "\"consecutiveExceptionCountToleratedForWrites\": 5,"
                            + "}");
        } else {
            logger.info("CosmosClient not configured with per partition circuit breaker");
        }

        GatewayConnectionConfig gatewayConnectionConfig = buildGatewayConnectionConfig();

        if (CONNECTION_MODE_AS_STRING.equals("DIRECT")) {
            logger.info("Creating client in direct mode");
            cosmosClientBuilder = cosmosClientBuilder.directMode(new DirectConnectionConfig(), gatewayConnectionConfig);
        } else if (CONNECTION_MODE_AS_STRING.equals("GATEWAY")) {
            logger.info("Creating client in gateway mode");
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

        cosmosAsyncClient = cosmosClientBuilder
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();

        //  Create initial database and container if they don't exist
        setupDBAndContainer();

        //  Insert initial data for the workload
        insertData();

        //  Start the workload
        startWorkload();

        //  Inject partition migration fault if enabled
        injectPartitionMigrationFault();

        while (true) {}
    }

    private static void startWorkload() {

        AtomicBoolean shouldStop = new AtomicBoolean(false);

        if (IS_DELAY_BETWEEN_OPERATIONS_ENABLED) {
            Mono.just(1)
                    .flatMap(integer -> {
                        boolean shouldStopSnapshot = shouldStop.get();
                        shouldStop.set(!shouldStopSnapshot);

                        if (shouldStopSnapshot) {
                            logger.info("Stopping workload");
                        } else {
                            logger.info("Resuming workload");
                        }

                        return Mono.empty();
                    })
                    .repeatWhen(longFlux -> longFlux.delayElements(Duration.ofSeconds(DELAY_BETWEEN_OPERATIONS_IN_SECONDS)))
                    .subscribeOn(FAULT_INJECTION_SCHEDULER)
                    .subscribe();
        }

        Mono.just(1)
                .repeat()
                .flatMap(integer -> {
                    int random = ThreadLocalRandom.current().nextInt(3);
                    switch (random) {
                        case 0:
                            return upsertItem(shouldStop);
                        case 1:
                            return readItem(shouldStop);
                        case 2:
                            return queryItem(shouldStop);
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

    private static Mono<CosmosItemResponse<Pojo>> upsertItem(AtomicBoolean shouldStop) {

        if (shouldStop.get() || IS_UPSERT_EXCLUDED) {
            logger.debug("Create operation is excluded from the workload");
            return Mono.empty();
        }

        final int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);
        final Pojo item = getItem(finalI, finalI);
        final PartitionKey partitionKey = new PartitionKey(item.getId());
        logger.debug("upsert item: {}", finalI);

        return cosmosAsyncContainer.upsertItem(item, partitionKey, POINT_REQ_OPTS)
                        .onErrorResume(throwable -> {
                            logger.error("Error occurred while upserting item", throwable);

                            if (throwable instanceof CosmosException) {
                                CosmosException cosmosException = (CosmosException) throwable;
                                logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());

                                if (cosmosException.getStatusCode() == 400) {
                                    logger.error("Item: {}; PartitionKey: {}", item, partitionKey);
                                }
                            }

                            return Mono.empty();
                        });
    }

    private static Mono<CosmosItemResponse<Pojo>> readItem(AtomicBoolean shouldStop) {

        if (shouldStop.get() || IS_READ_EXCLUDED) {
            logger.debug("Read operation is excluded from the workload");
            return Mono.empty();
        }

        final int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);
        logger.debug("read item: {}", finalI);
        final Pojo item = getItem(finalI, finalI);
        final PartitionKey partitionKey = new PartitionKey(item.getId());

        CosmosItemRequestOptions resultantItemRequestOptions = IS_AVAILABILITY_STRATEGY_ENABLED_FOR_READS
                ? POINT_REQ_OPTS_WITH_AVAILABILITY_STRATEGY
                : POINT_REQ_OPTS;

        return cosmosAsyncContainer.readItem(item.getId(), partitionKey, resultantItemRequestOptions, Pojo.class)
                        .doOnSuccess(readItem -> {
                            logger.debug("Item read successfully: {}", readItem.getItem().getId());
                        })
                        .onErrorResume(throwable -> {
                            logger.error("Error occurred while reading item", throwable);

                            if (throwable instanceof CosmosException) {
                                CosmosException cosmosException = (CosmosException) throwable;
                                logger.error("CosmosException: {} - {}", cosmosException.getStatusCode(), cosmosException.getDiagnostics().getDiagnosticsContext());
                            }

                            return Mono.empty();
                        });
    }

    private static Mono<List<Pojo>> queryItem(AtomicBoolean shouldStop) {

        if (shouldStop.get() || IS_QUERY_EXCLUDED) {
            logger.debug("Query operation is excluded from the workload");
            return Mono.empty();
        }

        int finalI = ThreadLocalRandom.current().nextInt(TOTAL_NUMBER_OF_DOCUMENTS);
        logger.debug("query item: {}", finalI);
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

    private static void insertData() {
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

    private static GatewayConnectionConfig buildGatewayConnectionConfig() {
        return new GatewayConnectionConfig().setIdleConnectionTimeout(Duration.ofSeconds(IDLE_CONNECTION_TIMEOUT_IN_SECONDS));
    }

    private static void injectPartitionMigrationFault() {
        if (IS_PARTITION_MIGRATION_FAULT_INJECTION_ENABLED) {
            logger.info("Injecting partition migration fault");
            // Implement fault injection logic here
            Mono.just(1)
                    .publishOn(FAULT_INJECTION_SCHEDULER)
                    .flatMap(ignore -> {
                        if (true) {
                            logger.info("Attempting to inject partition migration fault");
                            try {
                                return injectPartitionIsMigratingFault();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        } else {
                            logger.error("Workload has ended!");
                            System.exit(1);
                        }
                        return null;
                    })
                    .repeatWhen(longFlux -> longFlux.delayElements(Duration.ofMinutes(20)))
                    .onErrorComplete()
                    .subscribeOn(FAULT_INJECTION_SCHEDULER)
                    .subscribe();
        } else {
            logger.info("Partition migration fault injection is disabled");
        }
    }

    private static Mono<Void> injectPartitionIsMigratingFault() throws IOException {
        return CosmosFaultInjectionHelper.configureFaultInjectionRules(cosmosAsyncContainer, FAULT_INJECTION_RULES);
    }
}
