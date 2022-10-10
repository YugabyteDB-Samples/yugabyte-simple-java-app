package common.transactionImpl;

import common.SQLEnum;
import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

    @Override
    protected void YSQLExecute(Connection conn) {
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.StockLevelTransaction1.SQL, W_ID, D_ID));
            int D_NEXT_O_ID = -1;
            while (rs.next()) {
                D_NEXT_O_ID = rs.getInt(1);
                System.out.printf("D_NEXT_O_ID=%d\n",D_NEXT_O_ID);
            }
            int N = D_NEXT_O_ID + 1;
            rs = conn.createStatement().executeQuery(String.format(SQLEnum.StockLevelTransaction2.SQL, W_ID, D_ID, N, L, N, T));
            while (rs.next()) {
                int cnt = rs.getInt(1);
                System.out.printf("Count=%d\n",cnt);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
