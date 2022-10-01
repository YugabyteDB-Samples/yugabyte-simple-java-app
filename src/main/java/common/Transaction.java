package common;

import java.sql.Time;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Timer;
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

    public void execute() {
        beforeActuallyExecute();
        actuallyExecute();
        postActuallyExecute();
    }

    protected void beforeActuallyExecute() {
        startTimeStamp = System.currentTimeMillis();
        System.out.printf("Transaction begins");
    }

    protected void actuallyExecute() {

    }

    protected void postActuallyExecute() {
        long endTimeStamp = System.currentTimeMillis();
        long minitues = TimeUnit.MICROSECONDS.toMinutes(endTimeStamp - startTimeStamp);
        System.out.printf("%s completes,takes %d minitues",transactionType,minitues);
    }
}
