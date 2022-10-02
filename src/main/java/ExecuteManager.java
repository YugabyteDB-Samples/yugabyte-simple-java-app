import common.Transaction;

import java.sql.Connection;
import java.util.List;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 5:28 PM
 */
public class ExecuteManager {
    public void executeCommands(Connection conn, List<Transaction> list) {
        if (list == null) return;
        for (Transaction transaction : list) {
            transaction.execute(conn);
        }
    }

    public void report() {
    }
}
