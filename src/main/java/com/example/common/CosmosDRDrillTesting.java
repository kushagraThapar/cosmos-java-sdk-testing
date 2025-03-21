package com.example.common;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.implementation.apachecommons.lang.StringUtils;
import com.azure.cosmos.implementation.guava25.base.Strings;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class CosmosDRDrillTesting {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDRDrillTesting.class.getName());

    private static final String DATABASE_ID = System.getProperty("DATABASE_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("DATABASE_ID")), "MigrationDB"));
    private static final String CONTAINER_ID = System.getProperty("CONTAINER_ID",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("CONTAINER_ID")), "MigrationContainer"));
    private static final String PARTITION_KEY_PATH = "/pk";
    private static final String TOTAL_OPERATIONS = System.getProperty("TOTAL_OPERATIONS",
        StringUtils.defaultString(Strings.emptyToNull(
            System.getenv().get("TOTAL_OPERATIONS")), "100000"));
    private static final int TOTAL_NUMBER_OF_OPERATIONS = Integer.parseInt(TOTAL_OPERATIONS);

    private static final ScheduledThreadPoolExecutor scheduledExecutor =
        new ScheduledThreadPoolExecutor(Runtime.getRuntime().availableProcessors());

    private static CosmosAsyncClient cosmosAsyncClient;
    private static CosmosAsyncDatabase cosmosAsyncDatabase;
    private static CosmosAsyncContainer cosmosAsyncContainer;

    public static void main(String[] args) {
        cosmosAsyncClient = new CosmosClientBuilder()
            .endpoint(Configurations.endpoint)
            .key(Configurations.key)
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();

        //  Create initial database and container if they don't exist
        setupDBAndContainer();

        //  Insert initial data for the workload
        insertData();

        //  Start the workload
        startWorkload();
    }

    private static void startWorkload() {
        while (true) {
            int randomOperation = new Random().nextInt(4);
            switch (randomOperation) {
                case 1:
                    upsertItem();
                    break;
                case 2:
                    readItem();
                    break;
                case 3:
                    queryItem();
                    break;
                default:
            }
        }
    }

    private static void upsertItem() {
        try {
            int finalI = new Random().nextInt(TOTAL_NUMBER_OF_OPERATIONS);
            logger.info("upsert item: {}", finalI);
            scheduledExecutor.execute(() -> cosmosAsyncContainer.upsertItem(getItem(finalI, finalI),
                new CosmosItemRequestOptions()).block());
        } catch (Exception e) {
            logger.error("Error occurred while upserting item", e);
        }
    }

    private static void readItem() {
        try {
            int finalI = new Random().nextInt(TOTAL_NUMBER_OF_OPERATIONS);
            logger.info("read item: {}", finalI);
            Pojo item = getItem(finalI, finalI);
            scheduledExecutor.execute(() -> cosmosAsyncContainer.readItem(item.getId(), new PartitionKey(item.getPk()),
                Pojo.class).block());
        } catch (Exception e) {
            logger.error("Error occurred while reading item", e);
        }
    }

    private static void queryItem() {
        try {
            int finalI = new Random().nextInt(TOTAL_NUMBER_OF_OPERATIONS);
            logger.info("query item: {}", finalI);
            Pojo item = getItem(finalI, finalI);
            String query = "select * from c where c.id=@id and c.pk=@pk";
            SqlQuerySpec querySpec = new SqlQuerySpec(query);
            querySpec.setParameters(Arrays.asList(new SqlParameter("@id", item.getId()), new SqlParameter("@pk",
                item.getPk())));
            scheduledExecutor.execute(() -> cosmosAsyncContainer.queryItems(querySpec, Pojo.class).collectList().block());
        } catch (Exception e) {
            logger.error("Error occurred while querying item", e);
        }
    }

    private static void insertData() {
        logger.info("Inserting data...");
        for (int i = 0; i < TOTAL_NUMBER_OF_OPERATIONS; i++) {
            int finalI = i;
            scheduledExecutor.execute(() -> cosmosAsyncContainer.upsertItem(getItem(finalI, finalI),
                new CosmosItemRequestOptions()).block());
            if (finalI % 1000 == 0) {
                logger.info("Inserted {} records", finalI);
            }
        }
        logger.info("Inserted data...");
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
