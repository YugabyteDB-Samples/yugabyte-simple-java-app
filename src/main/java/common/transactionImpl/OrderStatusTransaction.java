package common.transactionImpl;

import common.SQLEnum;
import common.Transaction;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

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
    protected void YCQLExecute(Connection conn) {
    }

    @Override
    protected void YSQLExecute(Connection conn) {
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.OrderStatusTransaction1.SQL, C_W_ID,C_D_ID,C_ID));
            while (rs.next()) {
                String C_FIRST = rs.getString(1);
                String C_MIDDLE = rs.getString(2);
                String C_LAST = rs.getString(3);
                double C_BALANCE = rs.getDouble(4);
                System.out.printf("C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s,C_BALANCE=%f\n", C_FIRST, C_MIDDLE, C_LAST, C_BALANCE);
            }

            // get O_ID
            rs = conn.createStatement().executeQuery(String.format(SQLEnum.OrderStatusTransaction2.SQL, C_W_ID,C_D_ID,C_ID));
            List<Integer> O_IDs = new ArrayList<>();
            List<Timestamp> O_ENTRY_Ds = new ArrayList<>();
            List<Integer> O_CARRIER_IDs = new ArrayList<>();
            while (rs.next()) {
                int O_ID = rs.getInt(1);
                Timestamp O_ENTRY_D = rs.getTimestamp(2);
                int O_CARRIER_ID = rs.getInt(3);
                O_ENTRY_Ds.add(O_ENTRY_D);
                O_CARRIER_IDs.add(O_CARRIER_ID);
                O_IDs.add(O_ID);
            }
            for (int i = 0; i < O_IDs.size(); i++) {
                int O_ID = O_IDs.get(i);
                Timestamp O_ENTRY_D = O_ENTRY_Ds.get(i);
                int O_CARRIER_ID = O_CARRIER_IDs.get(i);
                System.out.printf("O_ID=%d,O_ENTRY_D=%s,O_CARRIER_ID=%d\n",O_ID,O_ENTRY_D,O_CARRIER_ID);
                String sql = String.format(SQLEnum.OrderStatusTransaction3.SQL,C_W_ID,C_D_ID,O_ID);
//                System.out.printf("SQL= %s\n",sql);
                ResultSet tmp = conn.createStatement().executeQuery(sql);
                while (tmp.next()) {
                    int OL_I_ID = tmp.getInt(1); // INT
                    int OL_SUPPLY_W_ID = tmp.getInt(2); // INT
                    int OL_QUANTITY = tmp.getInt(3); // DECIMAL(2,0);
                    double OL_AMOUNT = tmp.getDouble(4); // DECIMAL(6,2);
                    Timestamp OL_DELIVERY_D = tmp.getTimestamp(5); // TIMESTAMP
                    System.out.printf("OL_I_ID=%d,OL_SUPPLY_W_ID=%d,OL_QUANTITY=%d,OL_AMOUNT=%f,OL_DELIVERY_D=%s\n",OL_I_ID,OL_SUPPLY_W_ID,OL_QUANTITY,OL_AMOUNT,OL_DELIVERY_D);
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
