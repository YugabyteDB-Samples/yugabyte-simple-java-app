/**
 Copyright 2022 Yugabyte

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import com.yugabyte.ysql.YBClusterAwareDataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SampleApp {
    private static final String TABLE_NAME = "DemoAccount";

    public static void main(String[] args) {

        Properties settings = new Properties();
        try {
            settings.load(SampleApp.class.getResourceAsStream("app.properties"));
        }
        catch (IOException e) {
            e.printStackTrace();
            return;
        }

        YBClusterAwareDataSource ds = new YBClusterAwareDataSource();

        ds.setUrl("jdbc:yugabytedb://" + settings.getProperty("host") + ":"
            + settings.getProperty("port") + "/yugabyte");
        ds.setUser(settings.getProperty("dbUser"));
        ds.setPassword(settings.getProperty("dbPassword"));
        ds.setSsl(true);
        ds.setSslMode("verify-full");
        ds.setSslRootCert(settings.getProperty("sslRootCert"));

        try {
            Connection conn = ds.getConnection();
            System.out.println(">>>> Successfully connected to Yugabyte Cloud.");

            createDatabase(conn);

            selectAccounts(conn);
            transferMoneyBetweenAccounts(conn, 800);
            selectAccounts(conn);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createDatabase(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        DatabaseMetaData meta = conn.getMetaData();

        ResultSet resultSet = meta.getTables(null, null, TABLE_NAME.toLowerCase(),
            new String[] {"TABLE"});

        if (resultSet.next()) {
            System.out.println(">>>> Table " + TABLE_NAME + " already exists.");
            return;
        }

        stmt.execute("CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
            "(" +
            "id int PRIMARY KEY," +
            "name varchar," +
            "age int," +
            "country varchar," +
            "balance int" +
            ")");

        stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES" +
            "(1, 'Jessica', 28, 'USA', 10000)," +
            "(2, 'John', 28, 'Canada', 9000)");

        System.out.println(">>>> Successfully created " + TABLE_NAME + " table.");
    }

    private static void selectAccounts(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        System.out.println(">>>> Selecting accounts:");

        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);

        while (rs.next()) {
            System.out.println(String.format("name = %s, age = %s, country = %s, balance = %s",
                rs.getString(2), rs.getString(3),
                rs.getString(4), rs.getString(5)));
        }
    }

    private static void transferMoneyBetweenAccounts(Connection conn, int amount) throws SQLException {
        Statement stmt = conn.createStatement();

        try {
            stmt.execute(
                "BEGIN TRANSACTION;" +
                    "UPDATE " + TABLE_NAME + " SET balance = balance - " + amount + "" + " WHERE name = 'Jessica';" +
                    "UPDATE " + TABLE_NAME + " SET balance = balance + " + amount + "" + " WHERE name = 'John';" +
                    "COMMIT;"
            );
        } catch (SQLException e) {
            if (e.getErrorCode() == 40001) {
                // The operation aborted due to a concurrent transaction trying to modify the same set of rows
                // Consider adding retry logic for production-grade applications.
                e.printStackTrace();
            } else {
                throw e;
            }
        }

        System.out.println();
        System.out.println(">>>> Transferred " + amount + " between accounts.");
    }
}
