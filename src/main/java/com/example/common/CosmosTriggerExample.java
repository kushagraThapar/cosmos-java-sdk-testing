package com.example.common;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.CosmosTriggerProperties;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.TriggerOperation;
import com.azure.cosmos.models.TriggerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class CosmosTriggerExample {

    private static final Logger logger = LoggerFactory.getLogger(CosmosTriggerExample.class);

    private static final String DB_NAME = "testdb";
    private static final String CONTAINER_NAME = "testcontainer";
    private static CosmosAsyncClient cosmosAsyncClient;
    private static CosmosAsyncDatabase cosmosAsyncDatabase;
    private static CosmosAsyncContainer cosmosAsyncContainer;

    public static void main(String[] args) {
        setup();

        CosmosTriggerProperties cosmosTriggerProperties = getCosmosTriggerProperties();
        cosmosAsyncContainer.getScripts().createTrigger(cosmosTriggerProperties).block();

        TestItem testItem = getTestItem();
        cosmosAsyncContainer.createItem(testItem).block();

        TestItem item = cosmosAsyncContainer.readItem(testItem.getId(), new PartitionKey(testItem.getMypk()), TestItem.class).block().getItem();
        logger.info("Read item is : {}", item);
    }

    private static void setup() {
        cosmosAsyncClient = new CosmosClientBuilder()
            .endpoint(Configurations.endpoint)
            .key(Configurations.key)
            .buildAsyncClient();

        cosmosAsyncClient.createDatabaseIfNotExists(DB_NAME).block();
        cosmosAsyncDatabase = cosmosAsyncClient.getDatabase(DB_NAME);
        cosmosAsyncDatabase.createContainerIfNotExists(CONTAINER_NAME, "/mypk").block();
        cosmosAsyncContainer = cosmosAsyncDatabase.getContainer(CONTAINER_NAME);
    }

    private static TestItem getTestItem() {
        TestItem testItem = new TestItem(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        ZonedDateTime zonedDateTime = ZonedDateTime.now();
        OffsetDateTime offsetDateTime = OffsetDateTime.of(2023, 3, 31, 14, 12, 27, 800000000, ZoneOffset.ofHours(2));
        String formattedDate = offsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        testItem.setOffsetDateTime(offsetDateTime);
        testItem.setZonedDateTime(zonedDateTime);
        testItem.setOffsetDateTimeString(formattedDate);
        testItem.setZonedDateTimeString(DateTimeFormatter.ISO_ZONED_DATE_TIME.format(zonedDateTime));
        testItem.setDateTimeStringWithTrailingZeros(offsetDateTime.format(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        logger.info("Test item is : {}", testItem);
        return testItem;
    }

    private static CosmosTriggerProperties getCosmosTriggerProperties() {
        CosmosTriggerProperties trigger = new CosmosTriggerProperties(UUID.randomUUID().toString(),
            "function() {"
                + "var x = 10;"
                + "console.log(\"function executed\");"
                + "}");
        trigger.setTriggerOperation(TriggerOperation.CREATE);
        trigger.setTriggerType(TriggerType.PRE);

        return trigger;
    }
}
