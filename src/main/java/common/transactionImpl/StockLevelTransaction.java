package common.transactionImpl;

import common.Transaction;

import java.sql.Connection;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:19 AM
 */
public class StockLevelTransaction extends Transaction {
    int W_ID;
    int D_ID;
    int T;
    int L;

    public StockLevelTransaction(int w_ID, int d_ID, int t, int l) {
        W_ID = w_ID;
        D_ID = d_ID;
        T = t;
        L = l;
    }
//
//    @Override
//    protected void actuallyExecute(Connection conn) {
//        super.actuallyExecute(conn);
//    }

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

    public int getT() {
        return T;
    }

    public void setT(int t) {
        T = t;
    }

    public int getL() {
        return L;
    }

    public void setL(int l) {
        L = l;
    }
}
