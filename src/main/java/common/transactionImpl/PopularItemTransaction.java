package common.transactionImpl;

import common.SQLEnum;
import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

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
    protected void actuallyExecute(Connection conn) {
        System.out.printf("W_ID=%d,D_ID=%d,L=%d\n",W_ID,D_ID,L);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.PopularItemTransaction1.SQL, W_ID, D_ID, L));
            while (rs.next()) {
                int O_ID = rs.getInt(1);
                int O_ENTRY_D = rs.getInt(2);
                int C_FIRST = rs.getInt(3);
                int C_MIDDLE = rs.getInt(4);
                int C_LAST = rs.getInt(5);
                System.out.printf("O_ID=%d,O_ENTRY_D=%d,C_FIRST=%d,C_MIDDLE=%d,C_LAST=%d\n",O_ID,O_ENTRY_D,C_FIRST,C_MIDDLE,C_LAST);
            }

            rs = conn.createStatement().executeQuery(String.format(SQLEnum.PopularItemTransaction2.SQL, W_ID, D_ID, L));
            while (rs.next()) {
                int O_ID = rs.getInt(1);
                int I_NAME = rs.getInt(2);
                int OL_QUANTITY = rs.getInt(3);
                System.out.printf("O_ID=%d,I_NAME=%d,OL_QUANTITY=%d\n",O_ID,I_NAME,OL_QUANTITY);
            }

            rs = conn.createStatement().executeQuery(String.format(SQLEnum.PopularItemTransaction3.SQL, W_ID,D_ID,L,L));
            while (rs.next()) {
                int I_NAME = rs.getInt(1);
                double percentage = rs.getDouble(2);
                System.out.printf("I_NAME=%d,Percentage=%f\n",I_NAME, percentage);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
