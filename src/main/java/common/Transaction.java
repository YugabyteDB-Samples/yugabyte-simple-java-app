package common;

import com.datastax.oss.driver.api.core.CqlSession;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

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

    public long executeYSQL(Connection conn) throws SQLException {
        beforeExecute();
        YSQLExecute(conn);
        postExecute();
        return executionTime;
    }

    public long executeYCQL(CqlSession cqlSession) {
        beforeExecute();
        YCQLExecute(cqlSession);
        postExecute();
        return executionTime;
    }

    protected void beforeExecute() {
        startTimeStamp = System.currentTimeMillis();
        System.out.printf(transactionType.type + " Transaction begins\n");
    }

    protected void YSQLExecute(Connection conn) throws SQLException {

    }

    protected void YCQLExecute(CqlSession cqlSession) {

    }

    protected void postExecute() {
        long endTimeStamp = System.currentTimeMillis();
        long millis = TimeUnit.MILLISECONDS.toMillis(endTimeStamp - startTimeStamp);
        executionTime = millis;
        System.out.printf("%s completes,takes %d milliseconds\n",transactionType, millis);
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }
}
