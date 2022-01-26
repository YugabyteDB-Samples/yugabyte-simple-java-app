# Simple Java Application for YugabyteDB

The application connects to your YugabyteDB instance through the 
[topology-aware JDBC driver](https://docs.yugabyte.com/latest/integrations/jdbc-driver/) and performs basic SQL 
operations. The instructions below are provided for [Yugabyte Cloud](https://cloud.yugabyte.com/) deployments. 
If you are running YugabyteDB on your premises, then update the `/src/main/resources/app.properties` file with 
connectivity settings.

## Prerequisite
* Java Development Kit, version 8 or later
* Maven 3.0 or later
* Command line tool or your favourite IDE, such as IntelliJ IDEA, or Eclipse.

## Start Yugabyte Cloud Cluster

Unless you already have a cluster in Yugabyte Cloud, follow this simple 
[Quick Start Guide](https://docs.yugabyte.com/latest/yugabyte-cloud/cloud-quickstart/qs-add/) to provision a free
instance for this app and future experiments.

## Add Your Machine to IP allow list

The application will be running on your local laptop/machine and you need to add an IP address of the machine to the
[IP allow list](https://docs.yugabyte.com/latest/yugabyte-cloud/cloud-secure-clusters/add-connections/#manage-ip-allow-lists)
for your cluster instance. 

## Clone App From GitHub

Clone this app on your machine:

```bash
git clone https://github.com/yugabyte/yugabyte-simple-java-app.git
```

## Provide Yugabyte Cloud Settings

The application needs to establish a secured connection to your Yugabyte Cloud instance. To do that:
1. Open the `app.properties` file located in the following folder:
   ```bash
   {simple-java-app-yugabyte-cloud}/src/main/resources/app.properties
   ```
2. Edit the file by configuring the settings below:
   * `host` - the hostname of your Yugabyte Cloud instance.
   * `port` - the port number that will be used by the JDBC driver (the default is `5433`)
   * `dbUser` - the database username you used for your instance.
   * `dbPassword` - the database password.
   * `sslMode` - the SSL mode. Set to `verify-full` for Yugabyte Cloud deployments.
   * `sslRootCert` - a full path to your CA root cert (for example, `/Users/dmagda/certificates/root.crt`) 

Note, you can easily find all required settings through the Yugabyte Cloud UI:

![image](src/main/resources/cloud_app_settings.png)

## Build and Run App

1. Open a command line and navigate to the root directory of the project
    ```bash
   cd {location of the simple-java-app-yugabyte-cloud}
    ```
2. Build the app with Maven:
    ```bash
    mvn clean package
    ```
3. Run the app:
    ```bash
    java -cp target/yugabyte-simple-java-app-1.0-SNAPSHOT.jar SampleApp
    ```

Upon successful execution, the app prints out messages similar to the following:

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

Congrats! You successfully executed a simple Java app that works with Yugabyte Cloud. Now, let's look into the source 
code: 
1. Open the `SampleApp.java` located under the `simple-java-app-yugabyte-cloud/src/main/java/SampleApp.java` folder.
2. Check the `main` method that establishes a connection with your cloud instance via the topology-aware JDBC driver.
3. Look into the `createDatabase` method that uses Postgres-compliant DDL commands to create a sample database.
4. Check the `selectAccounts` method that queries your distributed data with so familiar SQL `SELECT` statement.
5. Explore the `transferMoneyBetweenAccounts` method that updates your data consistently with distributed transactions.

## Questions or Issues?

Having issues running this application or want to learn more from the expert who build and use Yugabyte?

Send a note to [our Slack channel](https://join.slack.com/t/yugabyte-db/shared_invite/zt-xbd652e9-3tN0N7UG0eLpsace4t1d2A),
or raise a question on StackOverflow and tag the question with `yugabytedb`!
