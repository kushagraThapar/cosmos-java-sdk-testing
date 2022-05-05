package com.example.common;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ModelBridgeInternal;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.example.common.Configurations.endpoint;
import static com.example.common.Configurations.key;

public class QueryThrottlingHangIssue {

    private final static Logger logger = LoggerFactory.getLogger(CosmosMetricsRegistryExample.class);

    public static void main(String[] args) {
        CosmosClient cosmosClient = new CosmosClientBuilder()
            .endpoint(endpoint)
            .key(key)
            .buildClient();

        CosmosContainer container = cosmosClient.getDatabase("testDB").getContainer("testContainer");
        CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
        ModelBridgeInternal.setQueryRequestOptionsMaxItemCount(options, 1);
        //        for (int i = 1; i < 10000; i++) {
        //            container.createItem(getObjectNode(String.valueOf(i), String.valueOf(10000 % i), "prop-" + (10000 % i)));
        //        }
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for(int i = 1; i < 30; i++) {
            executorService.submit(() -> {
                for (int j = 1; j < 5000; j++) {
                    String query = "select * from c where c.prop = 'prop-" + (10000 % j) + "'";
                    Iterator<ObjectNode> iterator = container.queryItems(query, options,
                        ObjectNode.class).iterator();
                    while (iterator.hasNext()) {
                        ObjectNode objectNode = iterator.next();
                        logger.info("Internal object node  - {}", objectNode.get("id"));
                    }
                }
            });
        }
        try {
            executorService.awaitTermination(10, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static ObjectNode getObjectNode(String id, String pkValue, String additionalProperty) {
        ObjectNode objectNode = new ObjectMapper().createObjectNode();
        objectNode.put("id", id);
        objectNode.put("mypk", pkValue);
        objectNode.put("prop", additionalProperty);
        return objectNode;
    }
}
