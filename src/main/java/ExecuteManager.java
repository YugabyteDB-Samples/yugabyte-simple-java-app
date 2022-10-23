import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * @Package PACKAGE_NAME
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 5:28 PM
 */
public class ExecuteManager {
    private Map<TransactionType, Statistics> map;
    private Set<TransactionType> skipSet;

    public ExecuteManager() {
        map = new HashMap<>();
        skipSet = new HashSet<>();
        for (TransactionType transactionType : TransactionType.values()) {
            map.put(transactionType, new Statistics(transactionType));
        }
//        skipSet.add(TransactionType.DELIVERY); // 7 minutes
        skipSet.add(TransactionType.NEW_ORDER); // not implemented yet
    }

    public void executeYSQL(Connection conn, List<Transaction> list) throws SQLException {
        System.out.printf("Execute YSQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            long executionTime = transaction.executeYSQL(conn);
            map.get(transaction.getTransactionType()).addNewData(executionTime);
            report();
        }
    }

    public void executeYCQL(CqlSession session, List<Transaction> list) {
        System.out.printf("Execute YCQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            long executionTime = transaction.executeYCQL(session);
            map.get(transaction.getTransactionType()).addNewData(executionTime);
            report();
        }
    }

    public void report() {
        System.out.println("---Statistics start---");
        for (Map.Entry<TransactionType, Statistics> entry : map.entrySet()) {
            System.out.println(entry.getValue());
        }
        System.out.println("---Statistics end---");
    }
}
