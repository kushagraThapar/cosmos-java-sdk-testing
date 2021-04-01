package com.example.common;

import static com.azure.cosmos.ConsistencyLevel.CONSISTENT_PREFIX;
import static com.azure.cosmos.ConsistencyLevel.STRONG;
import static com.microsoft.azure.documentdb.ConsistencyLevel.ConsistentPrefix;
import static com.microsoft.azure.documentdb.ConsistencyLevel.Strong;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.ExcludedPath;
import com.azure.cosmos.models.IncludedPath;
import com.azure.cosmos.models.IndexingMode;
import com.azure.cosmos.models.IndexingPolicy;
import com.azure.cosmos.models.ThroughputProperties;
import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.Database;
import com.microsoft.azure.documentdb.Document;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.DocumentCollection;
import com.microsoft.azure.documentdb.PartitionKey;
import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.ResourceResponse;
import com.microsoft.azure.documentdb.RetryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CosmosDbClientOldTest {

    private static final Logger logger = LoggerFactory.getLogger(CosmosDbClientOldTest.class);

	static final String DELIMETER = "->";

	public static void main(String[] srgs) {
		runV2SDK();
		runV4SDK();
	}

	private static void runV2SDK () {
        List<String> locations = new ArrayList<>();
        //		locations.add("North Central US");
        //		locations.add("East US");
        locations.add("West US 2");
        CosmosDbClientOldTest cosmosDbClient = new CosmosDbClientBuilder("host",
            "key", locations, false).build();
        		runOne(false, Strong, cosmosDbClient);
        		runOne(false, ConsistentPrefix, cosmosDbClient);
    }

    private static void runV4SDK () {
        List<String> locations = new ArrayList<>();
        //		locations.add("North Central US");
        //		locations.add("East US");
        locations.add("West US 2");
        CosmosDbClientOldTest cosmosDbClient = new CosmosDbClientBuilder("host",
            "key", locations, true).build();
        runOne(true, Strong, cosmosDbClient);
        runOne(true, ConsistentPrefix, cosmosDbClient);
    }

	public static class CosmosDbClientBuilder {
		private final String host;
		private final String key;
		private final Collection<String> locations;
		private int maxRetryAttempts = 1;
		private int maxWaitTimeInSeconds = 1;
		private int idleConnectionTimeout = 5;
		private boolean sdk;

		public CosmosDbClientBuilder(String host, String key, Collection<String> locations, boolean sdk) {
			this.host = host;
			this.key = key;
			this.locations = locations;
			this.sdk = sdk;
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
			return new CosmosDbClientOldTest(sdk, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, idleConnectionTimeout);
		}
	}

	static final String GROUP_ID = "subset";
	private DocumentClient cosmosClientStrong;
	private DocumentClient cosmosClientWeak;
	private CosmosClient cosmosSdkClientStrong;
	private CosmosClient cosmosSdkClientWeak;

	private CosmosDbClientOldTest(boolean sdk, String host, String key, Collection<String> locations, int maxRetryAttempts, int maxWaitTimeInSeconds, int idleConnectionTimeout) {
	    if (sdk) {
            this.cosmosSdkClientStrong = getCosmosClient(STRONG, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds);
            this.cosmosSdkClientWeak = getCosmosClient(CONSISTENT_PREFIX, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds);
        } else {
            this.cosmosClientStrong = getClient(Strong, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, idleConnectionTimeout);
            this.cosmosClientWeak = getClient(ConsistentPrefix, host, key, locations, maxRetryAttempts, maxWaitTimeInSeconds, idleConnectionTimeout);
        }
	}

	private static CosmosClient getCosmosClient(com.azure.cosmos.ConsistencyLevel consistencyLevel, String host, String key, Collection<String> locations,
			int maxRetryAttempts, int maxWaitTimeInSeconds) {
		ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
		retryOptions.setMaxRetryAttemptsOnThrottledRequests(maxRetryAttempts);
		retryOptions.setMaxRetryWaitTime(Duration.ofSeconds(maxWaitTimeInSeconds));
		CosmosClientBuilder cosmosClientBuilder = new CosmosClientBuilder()
				.endpoint(host).key(key)
				.throttlingRetryOptions(retryOptions)
				.multipleWriteRegionsEnabled(false)
				.consistencyLevel(consistencyLevel);
		DirectConnectionConfig conf = DirectConnectionConfig.getDefaultConfig();
		cosmosClientBuilder.directMode(conf);
		if (consistencyLevel != STRONG) {
			cosmosClientBuilder.preferredRegions(new ArrayList<>(locations));
			cosmosClientBuilder.endpointDiscoveryEnabled(true);
		}
		return cosmosClientBuilder.buildClient();
	}

	private static DocumentClient getClient(ConsistencyLevel consistencyLevel, String host, String key, Collection<String> locations,
			int maxRetryAttempts, int maxWaitTimeInSeconds, int idleConnectionTimeout) {
		ConnectionPolicy connectionPolicy = new ConnectionPolicy();
		RetryOptions ro = new RetryOptions();
		ro.setMaxRetryAttemptsOnThrottledRequests(maxRetryAttempts);
		ro.setMaxRetryWaitTimeInSeconds(maxWaitTimeInSeconds);
		connectionPolicy.setRetryOptions(ro);
		connectionPolicy.setUsingMultipleWriteLocations(false);
		connectionPolicy.setIdleConnectionTimeout(idleConnectionTimeout);
		connectionPolicy.setConnectionMode(ConnectionMode.DirectHttps);
		if (consistencyLevel != Strong) {
			connectionPolicy.setPreferredLocations(locations);
			connectionPolicy.setEnableEndpointDiscovery(true);
		}
		return new DocumentClient(host, key, connectionPolicy, consistencyLevel);
	}

    public void createDatabaseIfRequired(boolean sdk, String database) {
    	validateResourceId(database);
        int throughput = 400;
    	if (sdk) {
            CosmosDatabaseResponse res = cosmosSdkClientStrong.createDatabaseIfNotExists(database, ThroughputProperties.createManualThroughput(throughput));
            if (res.getStatusCode() != 200 && res.getStatusCode() != 201) {
                throw new RuntimeException("Cannot create database.");
            }
        } else {
    	    Database database1 = new Database();
    	    database1.setId(database);
    	    RequestOptions requestOptions = new RequestOptions();
    	    requestOptions.setOfferThroughput(throughput);
            try {
                ResourceResponse<Database> databaseResourceResponse =
                    cosmosClientStrong.readDatabase("dbs/" + database, new RequestOptions());
                if (databaseResourceResponse.getResource() == null) {
                    cosmosClientStrong.createDatabase(database1, requestOptions);
                }
            } catch (DocumentClientException e) {
                throw new RuntimeException("Cannot create database.", e);
            }
        }
    }

    public void createNamespaceIfRequired(boolean sdk, String database, String namespace) {
    	validateResourceId(database + namespace);
    	if (sdk) {
            CosmosContainerProperties containerProperties = new CosmosContainerProperties(namespace, "/id");

            IndexingPolicy indexingPolicy = new IndexingPolicy();
            indexingPolicy.setIndexingMode(IndexingMode.CONSISTENT);
            List<ExcludedPath> excludedPaths = new ArrayList<>();
            excludedPaths.add(new ExcludedPath("/*"));
            indexingPolicy.setExcludedPaths(excludedPaths);
            List<IncludedPath> includedPaths = new ArrayList<>();
            includedPaths.add(new IncludedPath("/" + GROUP_ID + "/*"));
            indexingPolicy.setIncludedPaths(includedPaths);
            containerProperties.setIndexingPolicy(indexingPolicy);

            containerProperties.setDefaultTimeToLiveInSeconds(-1);
            CosmosContainerResponse res = cosmosSdkClientStrong.getDatabase(database).createContainerIfNotExists(containerProperties);
            if (res.getStatusCode() != 200 && res.getStatusCode() != 201) {
                throw new RuntimeException("Cannot create namespace.");
            }
        } else {
            try {
                ResourceResponse<DocumentCollection> documentCollectionResourceResponse =
                    cosmosClientStrong.readCollection("dbs/" + database + "/colls/" + namespace, new RequestOptions());
                if (documentCollectionResourceResponse.getResource() == null) {
                    DocumentCollection documentCollection = new DocumentCollection();
                    documentCollection.setId(namespace);
                    com.microsoft.azure.documentdb.IndexingPolicy indexingPolicy = new com.microsoft.azure.documentdb.IndexingPolicy();
                    indexingPolicy.setIndexingMode(com.microsoft.azure.documentdb.IndexingMode.Consistent);
                    com.microsoft.azure.documentdb.IncludedPath includedPath = new com.microsoft.azure.documentdb.IncludedPath();
                    includedPath.setPath("/" + GROUP_ID + "/*");
                    com.microsoft.azure.documentdb.ExcludedPath excludedPath = new com.microsoft.azure.documentdb.ExcludedPath();
                    excludedPath.setPath("/*");
                    indexingPolicy.setIncludedPaths(Collections.singletonList(includedPath));
                    indexingPolicy.setExcludedPaths(Collections.singletonList(excludedPath));
                    documentCollection.setDefaultTimeToLive(-1);
                    documentCollection.setIndexingPolicy(indexingPolicy);
                    PartitionKeyDefinition partitionKeyDef = new PartitionKeyDefinition();
                    ArrayList<String> paths = new ArrayList<String>();
                    paths.add("/id");
                    partitionKeyDef.setPaths(paths);
                    documentCollection.setPartitionKey(partitionKeyDef);
                    cosmosClientStrong.createCollection("dbs/" + database, documentCollection, new RequestOptions());
                }
            } catch (DocumentClientException e) {
                throw new RuntimeException("Cannot create namespace.", e);
            }
        }
    }

	private static void runOne(boolean sdk, ConsistencyLevel consistency, CosmosDbClientOldTest cosmosDbClient) {
		String group = "group";
		String keyGen = "key4";
		String database = "444";
		String namespace = "maria";
		String value = "value123";

		int num = 200;
		int prewarm = 100;
		long getT = 0;
		long putT = 0;

		cosmosDbClient.createDatabaseIfRequired(sdk, database);
		cosmosDbClient.createNamespaceIfRequired(sdk, database, namespace);

		String sdkVersion = sdk ? "V4" : "V2";

		for (int i = 0; i < num; i++) {
			long start = System.currentTimeMillis();
			cosmosDbClient.writeValue(sdk, consistency, database, namespace, group, keyGen + i, value + i, -1);
			long time = (System.currentTimeMillis() - start);
			if (i > prewarm) putT = putT + time;
			if (i > 180)
				logger.info("PUT - SDK : {}, Consistency : {}, Time Taken : {}", sdkVersion, consistency, time);
//			try {
//				Thread.sleep(1000);
//			} catch (InterruptedException e) {
//			}
		}

		for (int i = 0; i < num; i++) {
			long start = System.currentTimeMillis();
			String res = cosmosDbClient.getValue(sdk, consistency, database, namespace, group, keyGen + i);
			long time = (System.currentTimeMillis() - start);
			if (i > prewarm) getT = getT + time;
			if (i > 180)
                logger.info("GET - SDK : {}, Consistency : {}, Time Taken : {}", sdkVersion, consistency, time);
//		try {
//			Thread.sleep(1000);
//		} catch (InterruptedException e) {
//		}
		}
		logger.info("Consistency : {}, SDK : {}, GET : {}, PUT : {}", consistency, sdkVersion, (getT / (num - prewarm)), (putT / (num - prewarm)));
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
				if (res.getStatusCode() == 404) {
					return null;
				}
				long duration = res.getDuration().toMillis();
				if (duration > 60) {
				    logger.info("READ TIME: {}, Diagnostics : {}", duration, res.getDiagnostics().toString());
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
			CosmosItemResponse<Item> res =
			getClientSdk(consistency).getDatabase(database).getContainer(namespace).upsertItem(item, new com.azure.cosmos.models.PartitionKey(item.getId()), options);
			long duration = res.getDuration().toMillis();
			if (duration > 100) {
			    logger.info("WRITE TIME: {}, Diagnostics : {}", duration, res.getDiagnostics().toString());
			}
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

