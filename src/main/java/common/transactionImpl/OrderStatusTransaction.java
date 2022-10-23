package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import common.Transaction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:06 AM
 */
// YSQL:72008ms , YCQL: 200ms
public class OrderStatusTransaction extends Transaction {
    int C_W_ID;
    int C_D_ID;
    int C_ID;

    @Override
    protected void YCQLExecute(CqlSession cqlSession) {
        ResultSet rs = null;
        List<Row> rows = null;

        // CQL1
        String CQL1 = String.format("select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from dbycql.Customer where C_W_ID = %d and C_D_ID = %d and C_ID = %d", C_W_ID, C_D_ID, C_ID);
        //select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from Customer where C_W_ID = 'C_W_ID' and C_D_ID = 'C_D_ID' and C_ID = 'C_ID' ;
        rs = cqlSession.execute(CQL1);
        rows = rs.all();
        for (Row row : rows) {
            String C_FIRST = row.getString(0);
            String C_MIDDLE = row.getString(1);
            String C_LAST = row.getString(2);
            BigDecimal C_BALANCE = row.getBigDecimal(3);
            System.out.printf("C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s,C_BALANCE=%f\n", C_FIRST, C_MIDDLE, C_LAST, C_BALANCE);
        }

        // CQL2
        String CQL2 = String.format("select O_ID, O_ENTRY_D, O_CARRIER_ID from dbycql.Orders where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d order by O_ID desc limit 1 allow filtering", C_W_ID, C_D_ID, C_ID);
        //select O_ID, O_ENTRY_D, O_CARRIER_ID from Orders where O_W_ID = 'C_W_ID' and O_D_ID = 'C_D_ID' and O_C_ID = 'C_ID' allow filtering order by O_ID desc limit 1
        rs = cqlSession.execute(CQL2);
        rows = rs.all();
        List<Integer> O_IDs = new ArrayList<>();
        List<Instant> O_ENTRY_Ds = new ArrayList<>();
        List<Integer> O_CARRIER_IDs = new ArrayList<>();
        for (Row row : rows) {
            int O_ID = row.getInt(0);
            Instant O_ENTRY_D = row.getInstant(1);
            Integer O_CARRIER_ID = row.getInt(2);
            O_ENTRY_Ds.add(O_ENTRY_D);
            O_CARRIER_IDs.add(O_CARRIER_ID);
            O_IDs.add(O_ID);
        }
        for (int i = 0; i < O_IDs.size(); i++) {
            int O_ID = O_IDs.get(i);
            Instant O_ENTRY_D = O_ENTRY_Ds.get(i);
            Integer O_CARRIER_ID = O_CARRIER_IDs.get(i);
            System.out.printf("O_ID=%d,O_ENTRY_D=%s,O_CARRIER_ID=%d\n", O_ID, O_ENTRY_D, O_CARRIER_ID);

            // CQL3
            String CQL3 = String.format("select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from dbycql.OrderLine where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d", C_W_ID, C_D_ID, O_ID);
            // select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from OrderLine where OL_W_ID = 'C_W_ID' OL_D_ID = 'C_D_ID' OL_O_ID = 'O_ID' ;
            rs = cqlSession.execute(CQL3);
            rows = rs.all();
            for (Row row : rows) {
                int OL_I_ID = row.getInt(0); // INT
                int OL_SUPPLY_W_ID = row.getInt(1); // INT
                BigDecimal OL_QUANTITY = row.getBigDecimal(2); // DECIMAL(2,0);
                BigDecimal OL_AMOUNT = row.getBigDecimal(3); // DECIMAL(6,2);
                Instant OL_DELIVERY_D = row.getInstant(4);
                System.out.printf("OL_I_ID=%d,OL_SUPPLY_W_ID=%d,OL_QUANTITY=%s,OL_AMOUNT=%s,OL_DELIVERY_D=%s\n", OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D);
            }
        }
    }

    @Override
    protected void YSQLExecute(Connection conn) throws SQLException {
        try {
            String SQL1 = "select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from Customer where C_W_ID = ? and C_D_ID = ? and C_ID = ?";
            PreparedStatement statement = conn.prepareStatement(SQL1);
            statement.setInt(1, C_W_ID);
            statement.setInt(2, C_D_ID);
            statement.setInt(3, C_ID);
            java.sql.ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                String C_FIRST = rs.getString(1);
                String C_MIDDLE = rs.getString(2);
                String C_LAST = rs.getString(3);
                double C_BALANCE = rs.getDouble(4);
                System.out.printf("C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s,C_BALANCE=%f\n", C_FIRST, C_MIDDLE, C_LAST, C_BALANCE);
            }

            // get O_ID
            String SQL2 = "select O_ID, O_ENTRY_D, O_CARRIER_ID from Orders where O_W_ID = ? and O_D_ID = ? and O_C_ID = ? order by O_ID desc limit 1";
            statement = conn.prepareStatement(SQL2);
            statement.setInt(1, C_W_ID);
            statement.setInt(2, C_D_ID);
            statement.setInt(3, C_ID);
            rs = statement.executeQuery();
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
                System.out.printf("O_ID=%d,O_ENTRY_D=%s,O_CARRIER_ID=%d\n", O_ID, O_ENTRY_D, O_CARRIER_ID);
                String SQL3 = "select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from OrderLine where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?";
                statement = conn.prepareStatement(SQL3);
                statement.setInt(1, C_W_ID);
                statement.setInt(2, C_D_ID);
                statement.setInt(3, O_ID);
                rs = statement.executeQuery();
                while (rs.next()) {
                    int OL_I_ID = rs.getInt(1); // INT
                    int OL_SUPPLY_W_ID = rs.getInt(2); // INT
                    int OL_QUANTITY = rs.getInt(3); // DECIMAL(2,0);
                    double OL_AMOUNT = rs.getDouble(4); // DECIMAL(6,2);
                    Timestamp OL_DELIVERY_D = rs.getTimestamp(5); // TIMESTAMP
                    System.out.printf("OL_I_ID=%d,OL_SUPPLY_W_ID=%d,OL_QUANTITY=%d,OL_AMOUNT=%f,OL_DELIVERY_D=%s\n", OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D);
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
