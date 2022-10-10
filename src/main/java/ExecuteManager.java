import common.Transaction;
import common.TransactionType;

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
        int cnt = 0;
        for (Transaction transaction : list) {
            if (!transaction.getTransactionType().equals(TransactionType.ORDER_STATUS)) continue;
            if (cnt > 5) continue;
            transaction.execute(conn);
            cnt++;
        }
    }

    public void report() {
    }
}
