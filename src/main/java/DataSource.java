import com.datastax.oss.driver.api.core.CqlSession;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 3:21 PM
 */
public class DataSource {
    private static CqlSession session;
    static {
        Properties settings = new Properties();
        try {
            settings.load(SampleApp.class.getResourceAsStream("app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        session = CqlSession
                .builder()
                .addContactPoint(new InetSocketAddress(settings.getProperty("host"), Integer.parseInt(settings.getProperty("port"))))
                .build();
    }

    private DataSource() {
    }

    public static CqlSession getSession(){
        return session;
    }
}
