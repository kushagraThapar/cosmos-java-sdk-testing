package com.example.common;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.models.PartitionKey;
import com.fasterxml.jackson.databind.JsonNode;

public class CosmosAfterburnerWarning {
    
    public static void main(String[] args) throws Exception {
        String endpoint = "<endpoint>";
        String key = "<key>";

        CosmosAsyncClient cosmosAsyncClient = new CosmosClientBuilder()
            .endpoint(endpoint)
            .key(key)
            .buildAsyncClient();

        //force an exception
        CosmosAsyncDatabase database = cosmosAsyncClient.getDatabase("non_existing_database");
        CosmosAsyncContainer container = database.getContainer("non_existing_container");
        container.readItem("itemId", new PartitionKey("partitionKey"), JsonNode.class).block();

    }
}