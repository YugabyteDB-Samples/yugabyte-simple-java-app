package common.transactionImpl;

import common.Transaction;

import java.sql.Connection;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 10:38 AM
 */
public class PaymentTranscaction extends Transaction {
    @Override
    protected void actuallyExecute(Connection conn) {
        super.actuallyExecute(conn);
    }
}
