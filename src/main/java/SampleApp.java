
/**
 * Copyright 2022 Yugabyte
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class SampleApp {
    private static final String TABLE_NAME = "DemoAccount";

    public static void main(String[] args) {
        try {
            Connection conn = DataSource.getConnection();
            System.out.println(">>>> Successfully connected to YugabyteDB!");

            createDatabase(conn);

            selectAccounts(conn);
            transferMoneyBetweenAccounts(conn, 800);
            selectAccounts(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createDatabase(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();

        stmt.execute("DROP TABLE IF EXISTS " + TABLE_NAME);

        stmt.execute("CREATE TABLE " + TABLE_NAME +
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
                            "UPDATE " + TABLE_NAME + " SET balance = balance - " + amount + ""
                            + " WHERE name = 'Jessica';" +
                            "UPDATE " + TABLE_NAME + " SET balance = balance + " + amount + "" + " WHERE name = 'John';"
                            +
                            "COMMIT;");
        } catch (SQLException e) {
            if (e.getSQLState().equals("40001")) {
                System.err.println("The operation is aborted due to a concurrent transaction that is" +
                        " modifying the same set of rows. Consider adding retry logic or using the pessimistic locking.");
                e.printStackTrace();
            } else {
                throw e;
            }
        }

        System.out.println();
        System.out.println(">>>> Transferred " + amount + " between accounts.");
    }
}
