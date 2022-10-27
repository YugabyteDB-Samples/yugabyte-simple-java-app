import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;
import jnr.ffi.annotations.In;

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
    private List<Statistics> transactionTypeList;
    private Map<TransactionType, Integer> skipMap;

    public ExecuteManager() {
        transactionTypeList = new ArrayList<>(8);
        skipSet = new HashSet<>(8);
        skipMap = new HashMap<>(8);

//        System.out.println(TransactionType.values());
        transactionTypeList.add(new Statistics(TransactionType.NEW_ORDER));
        transactionTypeList.add(new Statistics(TransactionType.PAYMENT));
        transactionTypeList.add(new Statistics(TransactionType.DELIVERY));
        transactionTypeList.add(new Statistics(TransactionType.ORDER_STATUS));
        transactionTypeList.add(new Statistics(TransactionType.STOCK_LEVEL));
        transactionTypeList.add(new Statistics(TransactionType.POPULAR_ITEM));
        transactionTypeList.add(new Statistics(TransactionType.TOP_BALANCE));
        transactionTypeList.add(new Statistics(TransactionType.RELATED_CUSTOMER));

        for (TransactionType transactionType : TransactionType.values()) {
            skipMap.put(transactionType, 0);
        }

        // 正选逻辑
//        skipSet.add(TransactionType.NEW_ORDER);
//        skipSet.add(TransactionType.DELIVERY);
//        skipSet.add(TransactionType.RELATED_CUSTOMER);

        // 反选逻辑
//        skipSet.addAll(Arrays.asList(TransactionType.values()));
//        skipSet.remove(TransactionType.NEW_ORDER);
    }

    public void executeYSQL(Connection conn, List<Transaction> list) throws SQLException {
        System.out.printf("Execute YSQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            int cnt = skipMap.get(transaction.getTransactionType());
            if (cnt >= 1) continue;
            skipMap.put(transaction.getTransactionType(), cnt+1);

            long executionTime = transaction.executeYSQL(conn);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            report();
        }
    }

    public void executeYCQL(CqlSession session, List<Transaction> list) {
        System.out.printf("Execute YCQL transactions\n");
        for (Transaction transaction : list) {
            if (skipSet.contains(transaction.getTransactionType())) continue;
            int cnt = skipMap.get(transaction.getTransactionType());
            if (cnt >= 1) continue;
            skipMap.put(transaction.getTransactionType(), cnt+1);
            
            long executionTime = transaction.executeYCQL(session);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            report();
        }
    }

    public void report() {
        System.out.println("---Statistics start---");
        for (Statistics statistics : transactionTypeList) {
            System.out.println(statistics);
        }
        System.out.println("---Statistics end---");
    }
}
