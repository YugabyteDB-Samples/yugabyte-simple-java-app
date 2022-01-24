# Simple Java Application for Yugabyte Cloud

The application connects to your [Yugabyte Cloud](https://cloud.yugabyte.com/) instance through the 
[topology-aware JDBC driver](https://docs.yugabyte.com/latest/integrations/jdbc-driver/) and performs basic SQL 
operations. Use the application as a template to get things started with Yugabyte Cloud in Java.

## Prerequisite
* Java Development Kit, version 8 or later
* Maven 3.0 or later
* Command line tool or your favourite IDE, such as IntelliJ IDEA, or Eclipse.

## Start Yugabyte Cloud Cluster

Unless you already have a cluster in Yugabyte Cloud, follow this simple 
[Quick Start Guide](https://docs.yugabyte.com/latest/yugabyte-cloud/cloud-quickstart/qs-add/) to provision a free
instance for this app and future experiments.

## Clone App From GitHub

Clone this app on your machine:

```bash
git clone https://github.com/yugabyte/simple-java-app-yugabyte-cloud.git
```

## Provide Yugabyte Cloud Settings

The application needs to know how to establish a secured connection with your Yugabyte Cloud instance. To do that:
1. Open the `app.properties` file located in the following folder:
   ```bash
   {simple-java-app-yugabyte-cloud}/src/main/resources/app.properties
   ```
2. Edit the file by configuring settings below:
   * `host` - the hostname of your Yugabyte Cloud instance.
   * `port` - the port number that will be used by the JDBC driver (the default is `5433`)
   * `dbUser` - the database username you used for your instance.
   * `dbPassword` - the database password.
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
    java -cp target/simple-java-app-yugabyte-cloud-1.0-SNAPSHOT.jar SampleApp
    ```

Upon successful execution, the app prints out messages similar to the following:

```bash
>>>> Successfully connected to Yugabyte Cloud.
>>>> Table DemoAccount already exists.
>>>> Selecting accounts:
name = Jessica, age = 28, country = USA, balance = 9200
name = John, age = 28, country = Canada, balance = 9800

>>>> Transferred 800 between accounts.
>>>> Selecting accounts:
name = Jessica, age = 28, country = USA, balance = 8400
name = John, age = 28, country = Canada, balance = 10600
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

Send us a note in Slack 
