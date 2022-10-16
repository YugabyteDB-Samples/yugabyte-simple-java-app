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
    public void executeYSQLCommands(Connection conn, List<Transaction> list) throws SQLException {
        if (list == null) return;
        for (Transaction transaction : list) {
            transaction.executeYSQL(conn);
        }
    }

    public void executeYCQLCommands(Connection conn, List<Transaction> list) {
        if (list == null) return;
        for (Transaction transaction : list) {
            transaction.executeYCQL(conn);
        }
    }

    public void report() {
    }
}
