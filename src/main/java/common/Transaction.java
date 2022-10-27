package common;

import com.datastax.oss.driver.api.core.CqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @Package common
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 4:02 PM
 */
public abstract class Transaction {
    // define template method for transaction execution
    TransactionType transactionType;
    private long startTimeStamp;
    private long executionTime;

    public long executeYSQL(Connection conn, Logger logger) throws SQLException {
        beforeExecute(logger);
        YSQLExecute(conn, logger);
        postExecute(logger);
        return executionTime;
    }

    public long executeYCQL(CqlSession cqlSession, Logger logger) {
        beforeExecute(logger);
        YCQLExecute(cqlSession, logger);
        postExecute(logger);
        return executionTime;
    }

    protected void beforeExecute(Logger logger) {
        startTimeStamp = System.currentTimeMillis();
       logger.log(Level.FINE, String.format(transactionType.type + " begins\n"));
    }

    protected void YSQLExecute(Connection conn, Logger logger) throws SQLException {

    }

    protected void YCQLExecute(CqlSession cqlSession, Logger logger) {

    }

    protected void postExecute(Logger logger) {
        long endTimeStamp = System.currentTimeMillis();
        long millis = TimeUnit.MILLISECONDS.toMillis(endTimeStamp - startTimeStamp);
        executionTime = millis;
       logger.log(Level.FINE, String.format("%s completes,takes %d milliseconds\n",transactionType, millis));
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }
}
