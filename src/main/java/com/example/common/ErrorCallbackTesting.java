package com.example.common;

import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.util.UUID;

public class ErrorCallbackTesting {
    private static final Logger logger = LoggerFactory.getLogger(ErrorCallbackTesting.class);

    public static void main(String[] args) throws InterruptedException {
        CosmosAsyncClient cosmosAsyncClient = new CosmosClientBuilder()
            .endpoint(Configurations.endpoint)
            .key(Configurations.key)
            .contentResponseOnWriteEnabled(true)
            .directMode(DirectConnectionConfig.getDefaultConfig().setMaxConnectionsPerEndpoint(1))
            .buildAsyncClient();

        cosmosAsyncClient.createDatabaseIfNotExists("testdb").block();
        CosmosAsyncDatabase cosmosAsyncDatabase = cosmosAsyncClient.getDatabase("testdb");
        cosmosAsyncDatabase.createContainerIfNotExists("testcontainer", "/pk").block();
        CosmosAsyncContainer cosmosAsyncContainer = cosmosAsyncDatabase.getContainer("testcontainer");

        Pojo pojo = createPojo();
        cosmosAsyncContainer.createItem(pojo).block();
        CosmosItemResponse<Pojo> response = cosmosAsyncContainer.readItem(pojo.getId(),
            new PartitionKey(pojo.getPk()), Pojo.class).block();
        logger.info("response is : {}", response);

        //  In this case, subscribe happened before attaching the error handlers,
        //  so the error in catch block will be Exceptions.ErrorCallbackNotImplemented
        try {
            Mono<CosmosItemResponse<Pojo>> obs = cosmosAsyncContainer.readItem("random", new PartitionKey(pojo.getPk()),
                Pojo.class);
            obs.subscribe();

            obs = obs.doOnNext(cosmosItemResponse -> {
                logger.info("1st response is : {}", cosmosItemResponse);
            }).onErrorResume(error -> {
                logger.error("1st onErrorResume - error is : {}", error.getMessage());
                return Mono.error(error);
            }).flatMap(r -> Mono.just(r));
            obs.block();
        } catch (Exception e) {
            logger.error("1st In catch block - error is : {}", e.getMessage());
        }

        //  In this case, we are attaching error handlers with subscribe,
        //  so the error in catch block will be CosmosException.
        //  we will NOT see ErrorCallbackNotImplementedException
        try {
            Mono<CosmosItemResponse<Pojo>> obs = cosmosAsyncContainer.readItem("random", new PartitionKey(pojo.getPk()),
                Pojo.class);

            obs.subscribe(cosmosItemResponse -> {
                logger.info("2nd In subscriber - response is : {}", cosmosItemResponse);
            }, error -> {
                logger.error("2nd In subscriber - error handling - error is : {}", error.getMessage());
            }, () -> {
                logger.info("2nd completed now");
            });

            obs = obs.doOnNext(cosmosItemResponse -> {
                logger.info("2nd response is : {}", cosmosItemResponse);
            }).onErrorResume(error -> {
                logger.error("2nd onErrorResume - error is : {}", error.getMessage());
                return Mono.error(error);
            }).flatMap(r -> Mono.just(r));
            obs.block();
        } catch (Exception e) {
            logger.error("2nd In catch block - error is : {}", e.getMessage());
        }

    }

    private static Pojo createPojo() {
        String id = UUID.randomUUID().toString();
        String pk = "pk";
        String field = "field-" + id;
        return new Pojo(id, pk, field);
    }
}
