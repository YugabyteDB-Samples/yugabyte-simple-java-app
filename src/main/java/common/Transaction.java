package common;

import java.sql.Connection;
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
    long startTimeStamp;

    public void execute(Connection conn) {
        beforeActuallyExecute();
        actuallyExecute(conn);
        postActuallyExecute();
    }

    protected void beforeActuallyExecute() {
        startTimeStamp = System.currentTimeMillis();
        System.out.printf("Transaction begins\n");
    }

    protected void actuallyExecute(Connection conn) {

    }

    protected void postActuallyExecute() {
        long endTimeStamp = System.currentTimeMillis();
        long seconds = TimeUnit.MICROSECONDS.toSeconds(endTimeStamp - startTimeStamp);
        System.out.printf("%s completes,takes %d seconds\n",transactionType, seconds);
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }
}
