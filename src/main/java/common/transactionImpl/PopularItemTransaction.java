package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import common.Transaction;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    protected void execute(CqlSession cqlSession) {
        ResultSet rs = null;
        List<Row> rows = null;

        System.out.printf("W_ID=%d,D_ID=%d,L=%d\n", W_ID, D_ID, L);

        // CQL1
        String CQL1 = String.format("select D_NEXT_O_ID from dbycql.District where D_W_ID = %d and D_ID = %d", W_ID, D_ID);
        rs = cqlSession.execute(CQL1);
        int N = rs.one().getInt(0);

        // CQL2
        String CQL2 = String.format("select O_C_ID, O_ID, O_ENTRY_D from dbycql.Orders where O_W_ID = %d and O_D_ID = %d and O_ID >= %d - %d and O_ID < %d", W_ID, D_ID, N, L, N);
        //-- 得到最新L个订单的信息 (O_C_ID, O_ID, O_ENTRY_D)
        rs = cqlSession.execute(CQL2);
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
            rs = cqlSession.execute(CQL3);
            Row onerow = rs.one();
            String C_FIRST = onerow.getString(0);
            String C_MIDDLE = onerow.getString(1);
            String C_LAST = onerow.getString(2);
            System.out.printf("O_ID=%d,O_ENTRY_D=%s,C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s\n", O_ID, O_ENTRY_D, C_FIRST, C_MIDDLE, C_LAST);

            // CQL4
            String CQL4 = String.format("select OL_W_ID, OL_D_ID, OL_O_ID, OL_QUANTITY from dbycql.OrderLine_popular where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d limit 1;", W_ID, D_ID, O_ID);
            rs = cqlSession.execute(CQL4);
            onerow = rs.one();
            int OL_O_ID = onerow.getInt(2);
            BigDecimal MAX_OL_QUANTITY = onerow.getBigDecimal(3);

            // CQL5
            // TODO:MAX_OL_QUANTITY 是浮点数，取出来后，再次查询，是否会出现精度损失呢？
            String CQL5 = String.format("select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID from dbycql.OrderLine_popular where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d and OL_QUANTITY = %f;", W_ID, D_ID, OL_O_ID, MAX_OL_QUANTITY);
            rs = cqlSession.execute(CQL5);
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
                rs = cqlSession.execute(CQL6);
                String I_NAME = rs.one().getString(0);
                System.out.printf("O_ID=%d,I_NAME=%s,MAX_OL_QUANTITY=%f\n", O_ID, I_NAME, MAX_OL_QUANTITY);
            }
        }

        for (int OL_I_ID : all_item_set) {
            // CQL7
            String CQL7 = String.format("select I_NAME from dbycql.Item where I_ID = %d;", OL_I_ID);
            rs = cqlSession.execute(CQL7);
            String I_NAME = rs.one().getString(0);

            // CQL8
            String CQL8 = String.format("select count(OL_I_ID) as I_NUM from dbycql.OrderLine_popular where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID >= %d-%d and OL_O_ID < %d and OL_I_ID = %d;", W_ID, D_ID, N, L, N, OL_I_ID);
            rs = cqlSession.execute(CQL8);
            // Exception in thread "main" com.datastax.oss.driver.api.core.DriverTimeoutException: Query timed out after PT2S
            int I_NUM = rs.one().getInt(0);
            double I_Percentage = I_NUM * 100.0 / L;
            System.out.printf("I_NAME=%s, I_Percentage=%f\n", I_NAME, I_Percentage);
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
