# Simple Java Application for YugabyteDB

The application connects to your YugabyteDB instance via 
[topology-aware JDBC driver](https://docs.yugabyte.com/latest/integrations/jdbc-driver/) and performs basic SQL 
operations. The instructions below are provided for [YugabyteDB Managed](https://cloud.yugabyte.com/) deployments.
If you use a different type of deployment, then update the `/src/main/resources/app.properties` file with proper connection parameters.

## Prerequisite
* Java Development Kit, version 8 or later
* Maven 3.0 or later
* Command line tool or your favourite IDE, such as IntelliJ IDEA, or Eclipse.

## Start YugabyteDB Managed Cluster

* [Start YugabyteDB Managed](https://docs.yugabyte.com/latest/yugabyte-cloud/cloud-quickstart/qs-add/) instance. Free tier exists.
* Add an IP address of your machine/laptop to the [IP allow list](https://docs.yugabyte.com/latest/yugabyte-cloud/cloud-secure-clusters/add-connections/#manage-ip-allow-lists)


## Build and Run App

1. Build the app with Maven:
    ```bash
    mvn clean package
    ```
2. Run the app:
    ```bash
    java -cp target/yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp
    ```

Upon successful execution, you will see output similar to the following:

```bash
>>>> Successfully connected to YugabyteDB!
>>>> Successfully created DemoAccount table.
>>>> Selecting accounts:
name = Jessica, age = 28, country = USA, balance = 10000
name = John, age = 28, country = Canada, balance = 9000

>>>> Transferred 800 between accounts.
>>>> Selecting accounts:
name = Jessica, age = 28, country = USA, balance = 9200
name = John, age = 28, country = Canada, balance = 9800
```
