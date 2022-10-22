import com.datastax.oss.driver.api.core.CqlSession;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.io.IOException;
import java.net.InetSocketAddress;
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
    private static HikariConfig config;
    private static HikariDataSource ds;

    private static CqlSession session;
    public static String MODE;
    public static final String YSQL = "YSQL";
    public static final String YCQL = "YCQL";

    static {
        Properties settings = new Properties();
        try {
            settings.load(SampleApp.class.getResourceAsStream("app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        MODE = settings.getProperty("mode");
        System.out.println("Your mode is " + MODE);
        if (MODE.equals(YSQL)) {
            Properties poolProperties = new Properties();
            poolProperties.setProperty("dataSourceClassName", "com.yugabyte.ysql.YBClusterAwareDataSource");
            poolProperties.setProperty("maximumPoolSize", "20");
            poolProperties.setProperty("dataSource.serverName", settings.getProperty("host"));
            poolProperties.setProperty("dataSource.portNumber", settings.getProperty("port_sql"));
            poolProperties.setProperty("dataSource.databaseName", "dbysql");
            poolProperties.setProperty("dataSource.user", settings.getProperty("dbUser"));
            poolProperties.setProperty("dataSource.password", settings.getProperty("dbPassword"));
            poolProperties.setProperty("poolName", "HikariCP");

            config = new HikariConfig(poolProperties);
            config.validate();
            String jdbcUrl = "jdbc:yugabytedb://" + settings.getProperty("host") + ":"
                    + settings.getProperty("port_sql") + "dbysql?load_balance=true";
            config.setJdbcUrl(jdbcUrl);

            ds = new HikariDataSource(config);
        } else if (MODE.equals(YCQL)) {
            session = CqlSession
                    .builder()
                    .addContactPoint(new InetSocketAddress(settings.getProperty("host"), Integer.parseInt(settings.getProperty("port_cql"))))
                    .build();
        }
        else throw new RuntimeException("mode in app.properties has to be YCQL/YSQL!");
    }


    // YCQL
    public static CqlSession getCQLSession() {
        return session;
    }

    private DataSource() {
    }

    // YSQL
    public static Connection getSQLConnection() throws SQLException {
        return ds.getConnection();
    }
}
