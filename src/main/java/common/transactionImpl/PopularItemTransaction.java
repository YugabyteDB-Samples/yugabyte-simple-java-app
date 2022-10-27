package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    protected void YCQLExecute(CqlSession cqlSession) {
        ResultSet rs = null;
        List<Row> rows = null;
        SimpleStatement simpleStatement = null;

        System.out.printf("W_ID=%d,D_ID=%d,L=%d\n", W_ID, D_ID, L);

        // CQL1
        String CQL1 = String.format("select D_NEXT_O_ID from dbycql.District where D_W_ID = %d and D_ID = %d", W_ID, D_ID);
//        rs = cqlSession.execute(CQL1);
        simpleStatement = SimpleStatement.builder(CQL1)
                .setExecutionProfileName("oltp")
                .build();
        rs = cqlSession.execute(simpleStatement);
        int N = rs.one().getInt(0);

        // CQL2
        String CQL2 = String.format("select O_C_ID, O_ID, O_ENTRY_D from dbycql.Orders where O_W_ID = %d and O_D_ID = %d and O_ID >= %d - %d and O_ID < %d", W_ID, D_ID, N, L, N);
        //-- 得到最新L个订单的信息 (O_C_ID, O_ID, O_ENTRY_D)
        rs = cqlSession.execute(CQL2);
        simpleStatement = SimpleStatement.builder(CQL2)
                .setExecutionProfileName("oltp")
                .build();
        rs = cqlSession.execute(simpleStatement);
        rows = rs.all();
        List<Integer> O_C_IDs = new ArrayList<>();
        List<Integer> O_IDs = new ArrayList<>();
        List<Instant> O_ENTRY_Ds = new ArrayList<>();
        for (Row row : rows) {
            int O_C_ID = row.getInt(0);
            int O_ID = row.getInt(1);
            Instant O_ENTRY_D = row.getInstant(2);
            O_C_IDs.add(O_C_ID);
            O_IDs.add(O_ID);
            O_ENTRY_Ds.add(O_ENTRY_D);
        }

        Set<Integer> all_item_set = new HashSet<>();
        //for every O_ID:
        for (int i = 0; i < O_IDs.size(); i++) {
            int O_ID = O_IDs.get(i);
            int O_C_ID = O_C_IDs.get(i);
            Instant O_ENTRY_D = O_ENTRY_Ds.get(i);

            // CQL3
            String CQL3 = String.format("select C_FIRST, C_MIDDLE, C_LAST from dbycql.Customer where C_W_ID = %d and C_D_ID = %d and C_ID = %d", W_ID, D_ID, O_C_ID);
//            rs = cqlSession.execute(CQL3);
            simpleStatement = SimpleStatement.builder(CQL3)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = cqlSession.execute(simpleStatement);

            Row onerow = rs.one();
            String C_FIRST = onerow.getString(0);
            String C_MIDDLE = onerow.getString(1);
            String C_LAST = onerow.getString(2);
            System.out.printf("O_ID=%d,O_ENTRY_D=%s,C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s\n", O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST);

            // CQL4
            String CQL4 = String.format("select OL_W_ID, OL_D_ID, OL_O_ID, OL_QUANTITY from dbycql.OrderLine where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d limit 1;", W_ID, D_ID, O_ID);
//            rs = cqlSession.execute(CQL4);
            simpleStatement = SimpleStatement.builder(CQL4)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = cqlSession.execute(simpleStatement);
            onerow = rs.one();
            int OL_O_ID = onerow.getInt(2);
            BigDecimal MAX_OL_QUANTITY = onerow.getBigDecimal(3);

            // CQL5
            String CQL5 = String.format("select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID from dbycql.OrderLine where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d and OL_QUANTITY = %f;", W_ID, D_ID, OL_O_ID, MAX_OL_QUANTITY);
//            rs = cqlSession.execute(CQL5);
            simpleStatement = SimpleStatement.builder(CQL5)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = cqlSession.execute(simpleStatement);
            List<Integer> OL_I_IDs = new ArrayList<>();
            rows = rs.all();
            for (Row row : rows) {
                int OL_W_ID = row.getInt(0);
                int OL_D_ID = row.getInt(1);
                int OL_I_ID = row.getInt(3);
                OL_I_IDs.add(OL_I_ID);
                all_item_set.add(OL_I_ID);
            }

            for (int OL_I_ID : OL_I_IDs) {
                // CQL6
                String CQL6 = String.format("select I_NAME from dbycql.Item where I_ID = %d;", OL_I_ID);
//                rs = cqlSession.execute(CQL6);
                simpleStatement = SimpleStatement.builder(CQL6)
                        .setExecutionProfileName("oltp")
                        .build();
                rs = cqlSession.execute(simpleStatement);
                String I_NAME = rs.one().getString(0);
                System.out.printf("O_ID=%d,I_NAME=%s,MAX_OL_QUANTITY=%f\n", O_ID, I_NAME, MAX_OL_QUANTITY);
            }
        }

        for (int OL_I_ID : all_item_set) {
            // CQL7
            String CQL7 = String.format("select I_NAME from dbycql.Item where I_ID = %d;", OL_I_ID);
//            rs = cqlSession.execute(CQL7);
            simpleStatement = SimpleStatement.builder(CQL7)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = cqlSession.execute(simpleStatement);
            String I_NAME = rs.one().getString(0);

            // CQL8
            String CQL8 = String.format("select count(OL_I_ID) as I_NUM from dbycql.OrderLine where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID >= %d-%d and OL_O_ID < %d and OL_I_ID = %d;", W_ID, D_ID, N, L, N, OL_I_ID);
            simpleStatement = SimpleStatement.builder(CQL8)
                    .setExecutionProfileName("oltp")
                    .build();
            rs = cqlSession.execute(simpleStatement);
            long I_NUM = rs.one().getLong(0);
            double I_Percentage = I_NUM * 100.0 / L;
            System.out.printf("I_NAME=%s, I_Percentage= %f%% \n", I_NAME, I_Percentage);
        }
    }

    @Override
    protected void YSQLExecute(Connection conn) throws SQLException {
        System.out.printf("W_ID=%d,D_ID=%d,L=%d\n", W_ID, D_ID, L);
        conn.setAutoCommit(false);
        try {
            // SQL1
            String SQL1 = "select D_NEXT_O_ID from District where D_W_ID = ? and D_ID = ?";
            // select D_NEXT_O_ID from District where D_W_ID = 'W_ID' and D_ID = 'D_ID'
            PreparedStatement statement = null;
            java.sql.ResultSet rs = null;
            int N = -1;
            statement = conn.prepareStatement(SQL1);
            statement.setInt(1, W_ID);
            statement.setInt(2, D_ID);
            rs = statement.executeQuery();
            while (rs.next()) {
                N = rs.getInt(1);
            }

            // SQL2
            String SQL2 = "with last_l_orders as ( select * from Orders where O_W_ID = ? and O_D_ID = ? and O_ID >= ? - ? and O_ID < ? ) select t1.O_ID, t1.O_ENTRY_D, t2.C_FIRST, t2.C_MIDDLE, t2.C_LAST from last_l_orders t1 left join Customer t2 on t1.O_W_ID = t2.C_W_ID and t1.O_D_ID = t2.C_D_ID and t1.O_ID = t2.C_ID";
            // with last_l_orders as ( select * from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N' ) select t1.O_ID, t1.O_ENTRY_D, t2.C_FIRST, t2.C_MIDDLE, t2.C_LAST from last_l_orders t1 left join Customer t2 on t1.O_W_ID = t2.C_W_ID and t1.O_D_ID = t2.C_D_ID and t1.O_ID = t2.C_ID ;
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

            // SQL3
            String SQL3 = "with last_l_orders as ( select * from Orders where O_W_ID = ? and O_D_ID = ? and O_ID >= ? - ? and O_ID < ? ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank = 1 order by t1.O_ID";
            // with last_l_orders as ( select * from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank = 1 order by t1.O_ID ;
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


            // SQL4
            String SQL4 = "with last_l_orders as ( select * from Orders where O_W_ID = ? and O_D_ID = ? and O_ID >= ? - ? and O_ID < ? ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t3.I_NAME, count(t2.OL_I_ID) * 100 / ? as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t3.I_NAME";
            // with last_l_orders as ( select * from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t3.I_NAME, count(t2.OL_I_ID) * 100 / 'L' as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t3.I_NAME ;
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
                System.out.printf("I_NAME=%s,Percentage=%f%%\n", I_NAME, percentage);
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
