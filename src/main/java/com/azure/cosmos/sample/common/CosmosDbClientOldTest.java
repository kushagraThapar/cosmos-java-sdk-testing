package com.azure.cosmos.sample.common;

import static com.azure.cosmos.ConsistencyLevel.CONSISTENT_PREFIX;
import static com.azure.cosmos.ConsistencyLevel.STRONG;
import static com.microsoft.azure.documentdb.ConsistencyLevel.ConsistentPrefix;
import static com.microsoft.azure.documentdb.ConsistencyLevel.Strong;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.RetryOptions;

public class CosmosDbClientOldTest {

    static final String DELIMETER = "->";

    public static void main(String[] srgs) {
        List<String> locations = new ArrayList<>();
        locations.add("West US 2");
        //  locations.add("East US");
        //  locations.add("North Central US");


        CosmosDbClientOldTest cosmosDbClient = new CosmosDbClientBuilder("account-host",
            "account-key", locations).build();

        runOne(true, ConsistentPrefix, cosmosDbClient);
        runOne(true, Strong, cosmosDbClient);
        runOne(false, ConsistentPrefix, cosmosDbClient);
        runOne(false, Strong, cosmosDbClient);
    }

    public static class CosmosDbClientBuilder {
        private final String host;
        private final String key;
        private final Collection<String> locations;
        private int maxRetryAttempts = 1;
        private int maxWaitTimeInSeconds = 1;
        private int idleConnectionTimeout = 5;

        public CosmosDbClientBuilder(String host, String key, Collection<String> locations) {
            this.host = host;
            this.key = key;
            this.locations = locations;
        }

        public CosmosDbClientBuilder addMaxRetryAttemptsOnThrottledRequests(int maxRetryAttempts) {
            if (maxRetryAttempts > 0) this.maxRetryAttempts = maxRetryAttempts;
            return this;
        }

        public CosmosDbClientBuilder addMaxRetryWaitTimeInSeconds(int maxWaitTimeInSeconds) {
            if (maxWaitTimeInSeconds > 0) this.maxWaitTimeInSeconds = maxWaitTimeInSeconds;
            return this;
        }

        public CosmosDbClientBuilder addIdleConnectionTimeout(int idleConnectionTimeout) {
            if (idleConnectionTimeout > 0) this.idleConnectionTimeout = idleConnectionTimeout;
            return this;
        }

        public CosmosDbClientOldTest build() {
            return new CosmosDbClientOldTest(host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, idleConnectionTimeout);
        }
    }

    static final String GROUP_ID = "subset";
    private final DocumentClient cosmosClientStrong;
    private final DocumentClient cosmosClientWeak;
    private final CosmosClient cosmosSdkClientStrong;
    private final CosmosClient cosmosSdkClientWeak;

    private CosmosDbClientOldTest(String host, String key, Collection<String> locations, int maxRetryAttempts, int maxWaitTimeInSeconds, int idleConnectionTimeout) {
        this.cosmosClientStrong = getClient(Strong, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, idleConnectionTimeout);
        this.cosmosClientWeak = getClient(ConsistentPrefix, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, idleConnectionTimeout);
        this.cosmosSdkClientStrong = getCosmosClient(STRONG, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, 5000);
        this.cosmosSdkClientWeak = getCosmosClient(CONSISTENT_PREFIX, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, 5000);
    }

    private static CosmosClient getCosmosClient(com.azure.cosmos.ConsistencyLevel consistencyLevel, String host, String key, Collection<String> locations,
                                                int maxRetryAttempts, int maxWaitTimeInSeconds, int maxPoolSize) {
        ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(maxRetryAttempts);
        retryOptions.setMaxRetryWaitTime(Duration.ofSeconds(maxWaitTimeInSeconds));
        CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
            .endpoint(host).key(key)
            .multipleWriteRegionsEnabled(false)
            .consistencyLevel(consistencyLevel);
        DirectConnectionConfig conf = DirectConnectionConfig.getDefaultConfig();
        cosmosClientBuilder.directMode(conf);
        cosmosClientBuilder.preferredRegions(new ArrayList<>(locations));
        cosmosClientBuilder.endpointDiscoveryEnabled(false);
        return cosmosClientBuilder.buildClient();
    }

    private static DocumentClient getClient(ConsistencyLevel consistencyLevel, String host, String key, Collection<String> locations,
                                            int maxRetryAttempts, int maxWaitTimeInSeconds, int idleConnectionTimeout) {
        ConnectionPolicy connectionPolicy = new ConnectionPolicy();
        connectionPolicy.setUsingMultipleWriteLocations(false);
        connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
        connectionPolicy.setPreferredLocations(locations);
        connectionPolicy.setEnableEndpointDiscovery(false);
        return new DocumentClient(host, key, connectionPolicy, consistencyLevel);
    }

    private static void runOne(boolean sdk, ConsistencyLevel consistency, CosmosDbClientOldTest cosmosDbClient) {
        String group = "group";
        String keyGen = UUID.randomUUID().toString();
        String database = "edbtest";
        String namespace = "ns";
        String value = "value";
        int num = 50;
        int prewarm = 5;
        long getT = 0;
        long putT = 0;

        for (int i = 0; i < num; i++) {
            long start = System.currentTimeMillis();
            cosmosDbClient.writeValue(sdk, consistency, database, namespace, group, keyGen + i, value + i, -1);
            long time = (System.currentTimeMillis() - start);
            if (i > prewarm) putT = putT + time;
            System.err.println(sdk + " " + consistency + " put: " + time);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//            }
        }

        for (int i = 0; i < num; i++) {
            long start = System.currentTimeMillis();
            String res = cosmosDbClient.getValue(sdk, consistency, database, namespace, group, keyGen + i);
            long time = (System.currentTimeMillis() - start);
            if (i > prewarm) getT = getT + time;
            System.err.println("get: " + time + " " + res);
//            try {
//                Thread.sleep(1000);
//            } catch (InterruptedException e) {
//            }
        }
        System.err.println(consistency + " " + (sdk ? "sdk" : "old") + " GET: " + (getT / (num - prewarm)) + " PUT: " + (putT / (num - prewarm)));
    }

    private static String createId(String group, String key) {
        if ((group + key).contains(DELIMETER))
            throw new IllegalArgumentException("Group or key contains a non-valid string " + DELIMETER);
        return group + DELIMETER + key;
    }

    private DocumentClient getClient(ConsistencyLevel consistency) {
        return consistency == Strong ? cosmosClientStrong : cosmosClientWeak;
    }

    private CosmosClient getClientSdk(ConsistencyLevel consistency) {
        return consistency == Strong ? cosmosSdkClientStrong : cosmosSdkClientWeak;
    }

    public static class Item {
        private String id;
        private String value;
        private Integer ttl;
        private String subset;
        private Long lastmodified;
        private Boolean deleted;

        public Item() {}

        public Item(String id, String value, Integer ttl, String subset, Long lastmodified, Boolean deleted) {
            this.id = id;
            this.value = value;
            this.ttl = ttl;
            this.subset = subset;
            this.lastmodified = lastmodified;
            this.deleted = deleted;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        public Integer getTtl() {
            return ttl;
        }

        public void setTtl(Integer ttl) {
            this.ttl = ttl;
        }

        public String getSubset() {
            return subset;
        }

        public void setSubset(String subset) {
            this.subset = subset;
        }

        public Long getLastmodified() {
            return lastmodified;
        }

        public void setLastmodified(Long lastmodified) {
            this.lastmodified = lastmodified;
        }

        public Boolean getDeleted() {
            return deleted;
        }

        public void setDeleted(Boolean deleted) {
            this.deleted = deleted;
        }
    }

    private String getValue(boolean sdk, ConsistencyLevel consistency, String database, String namespace, String group, String key) {
        String id = createId(group, key);
        if (sdk) {
            CosmosItemRequestOptions cosmosItemRequestOptions = new CosmosItemRequestOptions();
            cosmosItemRequestOptions.setConsistencyLevel(consistency == Strong ? STRONG : CONSISTENT_PREFIX);
            try {
                CosmosItemResponse<Item> res = getClientSdk(consistency).getDatabase(database).getContainer(namespace).readItem(id, new com.azure.cosmos.models.PartitionKey(id), cosmosItemRequestOptions, Item.class);
                System.out.println("Time taken to read item is: " + res.getDuration().toMillis());
                if (res.getStatusCode() == 404) {
                    return null;
                }
                Item item = res.getItem();
                return item.getValue();
            } catch (Exception e) {
                return null;
            }
        } else {
            String documentLink = String.format("dbs/%s/colls/%s/docs/%s/", database, namespace, id);

            RequestOptions ro = new RequestOptions();
            ro.setConsistencyLevel(consistency);
            ro.setPartitionKey(new PartitionKey(id));
            try {
                ResourceResponse<Document> res = getClient(consistency).readDocument(documentLink, ro);
                if (res.getStatusCode() == 404) {
                    return null;
                }
                return res.getResource().getString("value");
            } catch (Exception e) {
                return null;
            }
        }
    }

    private void writeDocumentDb(ConsistencyLevel consistency, String database, String namespace, String group, String key, String value, int ttlInSeconds) {
        String id = createId(group, key);
        Item item = new Item(id, value, ttlInSeconds, group, System.currentTimeMillis() / 1000, false);
        Document document = new Document();
        document.set(GROUP_ID, item.getSubset());
        document.set("id", item.getId());
        document.set("value", item.getValue());
        document.set("deleted", item.getDeleted());
        document.set("lastmodified", item.getLastmodified());
        document.setTimeToLive(item.getTtl());

        RequestOptions ro = new RequestOptions();
        ro.setConsistencyLevel(consistency);
        String collectionLink = String.format("dbs/%s/colls/%s", database, namespace);
        try {
            getClient(consistency).upsertDocument(collectionLink, document, ro, true);
        } catch (DocumentClientException e) {
            e.printStackTrace();
        }
    }

    private void writeSdk(ConsistencyLevel consistency, String database, String namespace, String group, String key, String value, int ttlInSeconds) {
        String id = createId(group, key);
        Item item = new Item(id, value, ttlInSeconds, group, System.currentTimeMillis() / 1000, false);
        try {
            CosmosItemRequestOptions options = new CosmosItemRequestOptions();
            options.setConsistencyLevel(consistency == Strong ? STRONG : CONSISTENT_PREFIX);
            CosmosItemResponse<Item> itemCosmosItemResponse =
                getClientSdk(consistency).getDatabase(database).getContainer(namespace).upsertItem(item,
                    new com.azure.cosmos.models.PartitionKey(item.getId()), options);
            System.out.println("Time taken to write is: " + itemCosmosItemResponse.getDuration().toMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeValue(boolean sdk, ConsistencyLevel consistency, String database, String namespace, String group,
                            String key, String value, int ttlInSeconds) {
        if (sdk) {
            writeSdk(consistency, database, namespace, group, key, value, ttlInSeconds);
        } else {
            writeDocumentDb(consistency, database, namespace, group, key, value, ttlInSeconds);
        }
    }

    static void validateResourceId(String id) {
        if (id.contains("/") || id.contains("\\") || id.contains("?") || id.contains("#"))
            throw new IllegalArgumentException("Resource id is not valid.");
    }
}

