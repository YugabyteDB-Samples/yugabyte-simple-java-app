# Simple Java Application for YugabyteDB

The application connects to your YugabyteDB instance via 
[topology-aware JDBC driver](https://docs.yugabyte.com/latest/integrations/jdbc-driver/) and performs basic SQL 
operations. The instructions below are provided for [Yugabyte Cloud](https://cloud.yugabyte.com/) deployments.
If you use a different type of deployment, then update the `/src/main/resources/app.properties` file with proper connection parameters.

## Prerequisite
* Java Development Kit, version 8 or later
* Maven 3.0 or later
* Command line tool or your favourite IDE, such as IntelliJ IDEA, or Eclipse.

## Start Yugabyte Cloud Cluster

* [Start YugabyteDB Cloud](https://docs.yugabyte.com/latest/yugabyte-cloud/cloud-quickstart/qs-add/) instance. Free tier exists.
* Add an IP address of your machine/laptop to the [IP allow list](https://docs.yugabyte.com/latest/yugabyte-cloud/cloud-secure-clusters/add-connections/#manage-ip-allow-lists)

## Clone App From GitHub

Clone the application to your machine:

```bash
git clone https://github.com/yugabyte/yugabyte-simple-java-app.git && cd yugabyte-simple-java-app
```

## Provide Yugabyte Cloud Connection Parameters

The application needs to establish a secured connection to your Yugabyte Cloud instance.

Open the `app.properties` file and specify the following configuration parameters:
* `host` - the hostname of your Yugabyte Cloud instance.
* `port` - the port number that will be used by the JDBC driver (the default is `5433`)
* `dbUser` - the database username you used for your instance.
* `dbPassword` - the database password.
* `sslMode` - the SSL mode. Set to `verify-full` for Yugabyte Cloud deployments.
* `sslRootCert` - a full path to your CA root cert (for example, `/Users/dmagda/certificates/root.crt`) 

Note, you can easily find all the settings on the Yugabyte Cloud dashboard:

![image](src/main/resources/cloud_app_settings.png)

## Build and Run App

1. Build the app with Maven:
    ```bash
    mvn clean package
    ```
2Run the app:
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

Congrats! You successfully executed a simple Java app that works with Yugabyte Cloud.

Now, explore the source code of `SampleApp.java` file:
1. `main` method - establishes a connection with your cloud instance via Go PostgreSQL driver.
3. `createDatabase` method - creates a table and populates it with sample data.
4. `selectAccounts` method - queries the data with SQL `SELECT` statements.
5. `transferMoneyBetweenAccounts` method - updates records consistently with distributed transactions.

## Questions or Issues?

Having issues running this application or want to learn more from the expert who build and use Yugabyte?

Send a note to [our Slack channel](https://join.slack.com/t/yugabyte-db/shared_invite/zt-xbd652e9-3tN0N7UG0eLpsace4t1d2A),
or raise a question on StackOverflow and tag the question with `yugabytedb`!
