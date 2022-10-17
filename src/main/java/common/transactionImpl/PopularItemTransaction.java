package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import common.SQLEnum;
import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:31 AM
 */
public class PopularItemTransaction extends Transaction {
    int W_ID;
    int D_ID;
    int L;

    @Override
    protected void execute(CqlSession cqlSession) {
    }

    public PopularItemTransaction(int w_ID, int d_ID, int l) {
        W_ID = w_ID;
        D_ID = d_ID;
        L = l;
    }

    public int getW_ID() {
        return W_ID;
    }

    public void setW_ID(int w_ID) {
        W_ID = w_ID;
    }

    public int getD_ID() {
        return D_ID;
    }

    public void setD_ID(int d_ID) {
        D_ID = d_ID;
    }

    public int getL() {
        return L;
    }

    public void setL(int l) {
        L = l;
    }
}
