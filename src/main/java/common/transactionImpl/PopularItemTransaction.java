package common.transactionImpl;

import common.SQLEnum;
import common.Transaction;

import java.sql.*;

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
    protected void YSQLExecute(Connection conn) {
        System.out.printf("W_ID=%d,D_ID=%d,L=%d\n", W_ID, D_ID, L);

        // SQL1
        String SQL1 = "select D_NEXT_O_ID from District where D_W_ID = ? and D_ID = ?";
        // select D_NEXT_O_ID from District where D_W_ID = 'W_ID' and D_ID = 'D_ID'
        PreparedStatement statement = null;
        ResultSet rs = null;
        int N = -1;
        try {
            statement = conn.prepareStatement(SQL1);
            statement.setInt(1, W_ID);
            statement.setInt(2, D_ID);
            rs = statement.executeQuery();
            while (rs.next()) {
                N = rs.getInt(1);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        // SQL2
        String SQL2 = "with last_l_orders as ( select * from Orders where O_W_ID = ? and O_D_ID = ? and O_ID >= ? - ? and O_ID < ? ) select t1.O_ID, t1.O_ENTRY_D, t2.C_FIRST, t2.C_MIDDLE, t2.C_LAST from last_l_orders t1 left join Customer t2 on t1.O_W_ID = t2.C_W_ID and t1.O_D_ID = t2.C_D_ID and t1.O_ID = t2.C_ID";
        // with last_l_orders as ( select * from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N' ) select t1.O_ID, t1.O_ENTRY_D, t2.C_FIRST, t2.C_MIDDLE, t2.C_LAST from last_l_orders t1 left join Customer t2 on t1.O_W_ID = t2.C_W_ID and t1.O_D_ID = t2.C_D_ID and t1.O_ID = t2.C_ID ;
        try {
            statement = conn.prepareStatement(SQL2);
            statement.setInt(1, W_ID);
            statement.setInt(2, D_ID);
            statement.setInt(3, N);
            statement.setInt(4, L);
            statement.setInt(5, N);
            rs = statement.executeQuery();
            while (rs.next()) {
                int O_ID = rs.getInt(1);
                Timestamp O_ENTRY_D = rs.getTimestamp(2);
                String C_FIRST = rs.getString(3);
                String C_MIDDLE = rs.getString(4);
                String C_LAST = rs.getString(5);
                System.out.printf("O_ID=%d,O_ENTRY_D=%s,C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s\n", O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        // SQL3
        String SQL3 = "with last_l_orders as ( select * from Orders where O_W_ID = ? and O_D_ID = ? and O_ID >= ? - ? and O_ID < ? ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank = 1 order by t1.O_ID";
        // with last_l_orders as ( select * from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank = 1 order by t1.O_ID ;
        try {
            statement = conn.prepareStatement(SQL3);
            statement.setInt(1, W_ID);
            statement.setInt(2, D_ID);
            statement.setInt(3, N);
            statement.setInt(4, L);
            statement.setInt(5, N);
            rs = statement.executeQuery();
            while (rs.next()) {
                int O_ID = rs.getInt(1);
                String I_NAME = rs.getString(2);
                int OL_QUANTITY = rs.getInt(3);
                System.out.printf("O_ID=%d,I_NAME=%s,OL_QUANTITY=%d\n", O_ID, I_NAME, OL_QUANTITY);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        // SQL4
        String SQL4 = "with last_l_orders as ( select * from Orders where O_W_ID = ? and O_D_ID = ? and O_ID >= ? - ? and O_ID < ? ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t3.I_NAME, count(t2.OL_I_ID) * 100 / ? as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t3.I_NAME";
        // with last_l_orders as ( select * from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t3.I_NAME, count(t2.OL_I_ID) * 100 / 'L' as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t3.I_NAME ;
        try {
            statement = conn.prepareStatement(SQL4);
            statement.setInt(1, W_ID);
            statement.setInt(2, D_ID);
            statement.setInt(3, N);
            statement.setInt(4, L);
            statement.setInt(5, N);
            statement.setInt(6, L);
            rs = statement.executeQuery();
            while (rs.next()) {
                String I_NAME = rs.getString(1);
                double percentage = rs.getDouble(2);
                System.out.printf("I_NAME=%s,Percentage=%f\n", I_NAME, percentage);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
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
