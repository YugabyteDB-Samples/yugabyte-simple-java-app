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
 * @Date 2/10/22 11:06 AM
 */
public class OrderStatusTransaction extends Transaction {
    int C_W_ID;
    int C_D_ID;
    int C_ID;

    @Override
    protected void YSQLExecute(Connection conn) {
//        String sql1 = String.format("select distinct C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from Customer where C_W_ID = %d, and C_D_ID = %d and C_ID = %d", C_W_ID, C_D_ID, C_ID);
//        String sql2 = String.format("select O_ID, O_ENTRY_D, O_CARRIER_ID from (select *, row_number()over(partition by O_W_ID, O_D_ID, O_C_ID order by O_ENTRY_D desc) as rank from Order where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d) t where rank = 1", C_W_ID, C_D_ID, C_ID);
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.OrderStatusTransaction1.SQL, C_W_ID,C_D_ID,C_ID));
            while (rs.next()) {
                int C_FIRST = rs.getInt(1);
                int C_MIDDLE = rs.getInt(2);
                int C_LAST = rs.getInt(3);
                int C_BALANCE = rs.getInt(4);
                System.out.printf("C_FIRST=%d,C_MIDDLE=%d,C_LAST=%d,C_BALANCE=%d\n", C_FIRST, C_MIDDLE, C_LAST, C_BALANCE);
            }

            // get O_ID
            rs = conn.createStatement().executeQuery(String.format(SQLEnum.OrderStatusTransaction2.SQL, C_W_ID,C_D_ID,C_ID));
            while (rs.next()) {
                int O_ID = rs.getInt(1);
                int O_ENTRY_D = rs.getInt(2);
                int O_CARRIER_ID = rs.getInt(3);
                // sql3
                System.out.printf("O_ID=%d\n");
                ResultSet tmp = conn.createStatement().executeQuery(String.format(SQLEnum.OrderStatusTransaction3.SQL,C_W_ID,C_D_ID,O_ID));
                while (tmp.next()) {
                    int OL_I_ID = tmp.getInt(1);
                    int OL_SUPPLY_W_ID = tmp.getInt(2);
                    int OL_QUANTITY = tmp.getInt(3);
                    int OL_AMOUNT = tmp.getInt(4);
                    int OL_DELIVERY_D = tmp.getInt(5);
                    System.out.printf("OL_I_ID=%d,OL_SUPPLY_W_ID=%d,OL_QUANTITY=%d,OL_AMOUNT=%d,OL_DELIVERY_D=%d\n",OL_I_ID,OL_SUPPLY_W_ID,OL_QUANTITY,OL_AMOUNT,OL_DELIVERY_D);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public OrderStatusTransaction(int c_W_ID, int c_D_ID, int c_ID) {
        C_W_ID = c_W_ID;
        C_D_ID = c_D_ID;
        C_ID = c_ID;
    }

    public int getC_W_ID() {
        return C_W_ID;
    }

    public void setC_W_ID(int c_W_ID) {
        C_W_ID = c_W_ID;
    }

    public int getC_D_ID() {
        return C_D_ID;
    }

    public void setC_D_ID(int c_D_ID) {
        C_D_ID = c_D_ID;
    }

    public int getC_ID() {
        return C_ID;
    }

    public void setC_ID(int c_ID) {
        C_ID = c_ID;
    }
}
