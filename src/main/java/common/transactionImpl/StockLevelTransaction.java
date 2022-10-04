package common.transactionImpl;

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
    protected void actuallyExecute(Connection conn) {
        String sql = String.format("with last_l_ol_orders as( select * from ( select *, rank()over(partition by OL_W_ID, C_D_ID order by OL_O_ID desc) as rank from OrderLine where OL_W_ID = %d and C_D_ID = %d ) t where rank < = %d ) select count(distinct S_I_ID) as item_cnt from last_l_ol_orders t1 left join Stock t2 on t1.OL_W_ID = t2.S_W_ID and t1.OL_I_ID = t2.S_I_ID where S_QUANTITY < %d", W_ID,D_ID,L,T);
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                int cnt = rs.getInt(1);
                System.out.println(cnt);
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
