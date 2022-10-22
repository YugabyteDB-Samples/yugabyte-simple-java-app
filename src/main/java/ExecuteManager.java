import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;
import org.apache.tinkerpop.gremlin.structure.T;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 5:28 PM
 */
public class ExecuteManager {
    public void executeYSQL(Connection conn, List<Transaction> list) throws SQLException {
        System.out.printf("Execute YSQL transactions\n");
        for (Transaction transaction : list) {
            transaction.executeYSQL(conn);
        }
    }

    public void executeYCQL(CqlSession session, List<Transaction> list) {
        System.out.printf("Execute YCQL transactions\n");
        for (Transaction transaction : list) {
            transaction.executeYCQL(session);
        }
    }

    public void report() {
    }
}
