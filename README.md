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
