import com.yugabyte.ysql.YBClusterAwareDataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Loader {

    public static void main(String[] args) {
        String host = "2e82c651-1d80-47f2-8cdf-07612b4fbda8.aws.ybdb.io";
        int port = 5433;
        String dbUser = "admin";
        String dbPassword = "DDkxo5ZhxzPgGTdxYrGfq7Ib-FRI0o";
        String sslRootCert = "/Users/dmagda/Downloads/yb_cloud/root.crt";

        YBClusterAwareDataSource ds = new YBClusterAwareDataSource();

        ds.setUrl("jdbc:yugabytedb://" + host + ":" + port + "/yugabyte");
        ds.setUser(dbUser);
        ds.setPassword(dbPassword);
        ds.setSsl(true);
        ds.setSslMode("verify-full");
        ds.setSslRootCert(sslRootCert);

        try {
            Connection conn = ds.getConnection();
            System.out.println(">>>> Successfully connected to Yugabyte Cloud!");

            createDatabase(conn);
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void createDatabase(Connection conn) throws SQLException {
        String tableName = "DemoAccount";

        Statement stmt = conn.createStatement();

        DatabaseMetaData meta = conn.getMetaData();

        ResultSet resultSet = meta.getTables(null, null, tableName.toLowerCase(),
            new String[] {"TABLE"});

        if (resultSet.next()) {
            System.out.println(">>>> Table " + tableName + " already exists.");
            return;
        }

        stmt.execute("CREATE TABLE IF NOT EXISTS " + tableName +
            "(" +
            "id int PRIMARY KEY," +
            "name varchar," +
            "age int," +
            "country varchar," +
            "balance int" +
            ")");

        stmt.execute("INSERT INTO " + tableName + " VALUES" +
            "(1, 'Jessica', 28, 'USA', 10000)," +
            "(2, 'John', 28, 'Canada', 9000)");

        System.out.println(">>>> Successfully created " + tableName + " table!");
    }
}
