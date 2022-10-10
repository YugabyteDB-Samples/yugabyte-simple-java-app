import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 3:21 PM
 */
public class DataSource {
    private static HikariConfig config = new HikariConfig();
    private static HikariDataSource ds;

    static {
        Properties poolProperties = new Properties();
        Properties settings = new Properties();
        try {
            settings.load(SampleApp.class.getResourceAsStream("app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        poolProperties.setProperty("dataSourceClassName", "com.yugabyte.ysql.YBClusterAwareDataSource");
        poolProperties.setProperty("maximumPoolSize", "20");
        poolProperties.setProperty("dataSource.serverName", settings.getProperty("host"));
        poolProperties.setProperty("dataSource.portNumber", settings.getProperty("port"));
        poolProperties.setProperty("dataSource.databaseName", "dbysql");
        poolProperties.setProperty("dataSource.user", settings.getProperty("dbUser"));
        poolProperties.setProperty("dataSource.password", settings.getProperty("dbPassword"));
        poolProperties.setProperty("poolName", "HikariCP");

        config = new HikariConfig(poolProperties);
        config.validate();
        String jdbcUrl = "jdbc:yugabytedb://" + settings.getProperty("host") + ":"
                + settings.getProperty("port") + "/yugabyte";
        config.setJdbcUrl(jdbcUrl);

        ds = new HikariDataSource(config);
    }

    private DataSource() {
    }

    public static Connection getConnection() throws SQLException {
        return ds.getConnection();
    }
}
