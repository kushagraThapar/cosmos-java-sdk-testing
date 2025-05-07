# Azure Cosmos java SDK testing repository

cosmos java SDK testing repository for experiments with Azure Cosmos DB Java SDK

### Cosmos DB DR Drill Testing workload
Steps to run Cosmos DB DR Drill Testing workload:

1. cd to the project folder
2. `mvn package` (build the fat jar with dependencies)
3. Running the drill

3.1 With Key-Based Authentication
`java -cp target/Java-SDK-testing-1.0-SNAPSHOT-jar-with-dependencies.jar -Dendpoint="account-endpoint" -Dkey="account-key" -DDATABASE_ID="database-id" -DCONTAINER_ID="container-id" -DTOTAL_DOCUMENTS="total-documents" com.example.common.CosmosDRDrillTesting` (run the jar with command line params)

3.2 With AAD Authentication
`java -cp target/Java-SDK-testing-1.0-SNAPSHOT-jar-with-dependencies.jar -Dendpoint="account-endpoint" -DDATABASE_ID="database-id" -DCONTAINER_ID="container-id" -DTOTAL_DOCUMENTS="total-documents" -DIS_MANAGED_IDENTITY_ENABLED="true" com.example.common.CosmosDRDrillTesting`

3.3 With Gateway Mode
`java -cp target/Java-SDK-testing-1.0-SNAPSHOT-jar-with-dependencies.jar -Dendpoint="account-endpoint" -DDATABASE_ID="database-id" -DCONTAINER_ID="container-id" -DTOTAL_DOCUMENTS="total-documents" -DIS_MANAGED_IDENTITY_ENABLED="true" -DCONNECTION_MODE="GATEWAY" com.example.common.CosmosDRDrillTesting`

4. Kill the workload when done.
