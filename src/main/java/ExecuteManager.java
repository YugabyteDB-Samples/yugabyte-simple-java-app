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
    private Set<TransactionType> skipSet;
    private List<TransactionType> transactionTypeList;
    private Map<TransactionType, Statistics> map;

    public ExecuteManager() {
        transactionTypeList = new ArrayList<>(8);
        map = new HashMap<>(8);
        skipSet = new HashSet<>(8);

        System.out.println(TransactionType.values());
        for (TransactionType transactionType : TransactionType.values()) {
            map.put(transactionType, new Statistics(transactionType));
        }
//        skipSet.add(TransactionType.DELIVERY); // 7 minutes
        skipSet.addAll(Arrays.asList(TransactionType.values()));
        skipSet.remove(TransactionType.POPULAR_ITEM);
    }

    public void executeYSQL(Connection conn, List<Transaction> list) throws SQLException {
        System.out.printf("Execute YSQL transactions\n");
//        for (Transaction transaction : list) {
//            if (skipSet.contains(transaction.getTransactionType())) continue;
//            long executionTime = transaction.executeYSQL(conn);
//            map.get(transaction.getTransactionType()).addNewData(executionTime);
//            report();
//        }
    }

    public void executeYCQL(CqlSession session, List<Transaction> list) {
        System.out.printf("Execute YCQL transactions\n");
//        for (Transaction transaction : list) {
//            if (skipSet.contains(transaction.getTransactionType())) continue;
//            long executionTime = transaction.executeYCQL(session);
//            map.get(transaction.getTransactionType()).addNewData(executionTime);
//            report();
//        }
    }

    public void report() {
        System.out.println("---Statistics start---");
        for (Map.Entry<TransactionType, Statistics> entry : map.entrySet()) {
            System.out.println(entry.getValue());
        }
        System.out.println("---Statistics end---");
    }
}
