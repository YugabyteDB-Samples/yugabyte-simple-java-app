import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;

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
    public void executeYCQLCommands(CqlSession cqlSession, List<Transaction> list) {
        if (list == null) return;
        System.out.printf("Execute YCQL transactions\n");
        for (Transaction transaction : list) {
            transaction.executeYCQL(cqlSession);
        }
    }

    public void report() {
    }
}
