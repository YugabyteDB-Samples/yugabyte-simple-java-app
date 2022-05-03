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

## Clone App From GitHub

Clone the application to your machine:

```bash
git clone https://github.com/yugabyte/yugabyte-simple-java-app.git && cd yugabyte-simple-java-app
```

## Provide Cluster Connection Parameters

The application needs to establish a secured connection to your YugabyteDB Managed instance.

Open the `app.properties` file and specify the following configuration parameters:
* `host` - the hostname of your cluster instance.
* `port` - the port number that will be used by the JDBC driver (the default is `5433`)
* `dbUser` - the database username you used for your instance.
* `dbPassword` - the database password.
* `sslMode` - the SSL mode. Set to `verify-full` for YugabyteDB Managed deployments.
* `sslRootCert` - a full path to your CA root cert (for example, `/Users/dmagda/certificates/root.crt`) 

Note, you can easily find all the settings on the YugabyteDB Managed dashboard:

![image](src/main/resources/cloud_app_settings.png)

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

## Explore App Logic

Congrats! You successfully executed a simple Java app that works with YugabyteDB.

Now, explore the source code of the `SampleApp.java` file:
1. `main` method - establishes a connection with your cloud instance via JDBC driver.
2. `createDatabase` method - creates a table and populates it with sample data.
3. `selectAccounts` method - queries the data with SQL `SELECT` statements.
4. `transferMoneyBetweenAccounts` method - updates records consistently with distributed transactions.

## Questions or Issues?

Having issues running this application or want to learn more from Yugabyte experts?

Join [our Slack channel](https://communityinviter.com/apps/yugabyte-db/register),
or raise a question on StackOverflow and tag the question with `yugabytedb`!
