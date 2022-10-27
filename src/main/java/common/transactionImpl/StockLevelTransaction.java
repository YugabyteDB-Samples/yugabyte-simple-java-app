package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import common.SQLEnum;
import common.Transaction;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:19 AM
 */
// YSQL: 43151ms, YCQL: 17213ms
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
    protected void YCQLExecute(CqlSession cqlSession) {
        ResultSet rs = null;
        List<Row> rows = null;

        // CQL1
        String CQL1 = String.format("select D_NEXT_O_ID from dbycql.District where D_W_ID = %d and D_ID = %d", W_ID, D_ID);
//        System.out.println(CQL1);
        //select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from Customer where C_W_ID = 'C_W_ID' and C_D_ID = 'C_D_ID' and C_ID = 'C_ID' ;
        rs = cqlSession.execute(CQL1);
        int N = rs.one().getInt(0);

        // CQL2
        String CQL2 = String.format("select OL_I_ID from dbycql.OrderLine where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID >= %d - %d and OL_O_ID < %d allow filtering", W_ID, D_ID, N, L, N);
//        System.out.println(CQL2);
        Set<Integer> OL_I_IDs = new HashSet<>();
        rs = cqlSession.execute(CQL2);
        rows = rs.all();
        for (Row row : rows) {
            int OL_I_ID = row.getInt(0);
            OL_I_IDs.add(OL_I_ID);
        }

        // CQL3
        int num = 0;
        for (int OL_I_ID : OL_I_IDs) {
            String CQL3 = String.format("select S_QUANTITY from dbycql.Stock where S_W_ID = %d and S_I_ID = %d allow filtering", W_ID, OL_I_ID);
//            System.out.println(CQL3);
            rs = cqlSession.execute(CQL3);
            BigDecimal S_QUANTITY = rs.one().getBigDecimal(0);
            double d = S_QUANTITY.doubleValue();
            if (d < T) num++;
        }
        System.out.println("num=" + num);
    }

    @Override
    protected void YSQLExecute(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
        try {
            java.sql.ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.StockLevelTransaction1.SQL, W_ID, D_ID));
            int D_NEXT_O_ID = -1;
            while (rs.next()) {
                D_NEXT_O_ID = rs.getInt(1);
                System.out.printf("D_NEXT_O_ID=%d\n", D_NEXT_O_ID);
            }
            int N = D_NEXT_O_ID + 1;
            rs = conn.createStatement().executeQuery(String.format(SQLEnum.StockLevelTransaction2.SQL, W_ID, D_ID, N, L, N, T));
            while (rs.next()) {
                int cnt = rs.getInt(1);
                System.out.printf("Count=%d\n", cnt);
            }

            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            if (conn != null) {
                System.err.print("Transaction is being rolled back\n");
                conn.rollback();
            }
        } finally {
            conn.setAutoCommit(true);
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
