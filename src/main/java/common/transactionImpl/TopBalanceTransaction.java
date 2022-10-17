package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:36 AM
 */
public class TopBalanceTransaction extends Transaction {
    @Override
    protected void execute(CqlSession cqlSession) {

    }
}
