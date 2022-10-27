import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

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
    private int counter;

    public ExecuteManager() {
        transactionTypeList = new ArrayList<>(8);
        skipSet = new HashSet<>(8);
        skipMap = new HashMap<>(8);
        counter = 0;

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
//        skipSet.remove(TransactionType.PAYMENT);
    }

    public void executeYSQL(Connection conn, List<Transaction> list, Logger logger) throws SQLException {
        logger.log(Level.INFO, "Execute YSQL transactions\n");
        for (Transaction transaction : list) {
//            if (skipSet.contains(transaction.getTransactionType())) continue;
//            int cnt = skipMap.get(transaction.getTransactionType());
//            if (cnt >= 1) continue;
//            skipMap.put(transaction.getTransactionType(), cnt+1);

            long executionTime = transaction.executeYSQL(conn, logger);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            report(logger);
        }
    }

    public void executeYCQL(CqlSession session, List<Transaction> list, Logger logger) {
        logger.log(Level.INFO, "Execute YCQL transactions\n");
        for (Transaction transaction : list) {
//            if (skipSet.contains(transaction.getTransactionType())) continue;
//            int cnt = skipMap.get(transaction.getTransactionType());
//            if (cnt >= 1) continue;
//            skipMap.put(transaction.getTransactionType(), cnt+1);

            long executionTime = transaction.executeYCQL(session, logger);
            transactionTypeList.get(transaction.getTransactionType().index).addNewData(executionTime);
            report(logger);
        }
    }

    public void report(Logger logger) {
        counter++; // print statistics every 5 transactions.
        if (counter % 5 == 0) {
            logger.log(Level.INFO, "---Statistics start---");
            for (Statistics statistics : transactionTypeList) {
                logger.log(Level.INFO, statistics.toString());
            }
            logger.log(Level.INFO, "---Statistics end---");
        }
    }
}
