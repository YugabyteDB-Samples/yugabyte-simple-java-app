package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import common.Transaction;

import java.math.BigDecimal;
import java.util.List;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:36 AM
 */
public class TopBalanceTransaction extends Transaction {
    @Override
    protected void execute(CqlSession cqlSession) {
        ResultSet rs = null;
        List<Row> rows = null;

        // CQL1
        String CQL1 = String.format("CREATE TABLE dbycql.customer_balance_top10 ( cb_top10 text, cb_w_id int, cb_d_id int, cb_id int, cb_first text, cb_middle text, cb_last text, cb_balance decimal, cb_time timeuuid, PRIMARY KEY ((cb_top10), cb_balance, cb_time) ) WITH CLUSTERING ORDER BY (cb_balance DESC, cb_time);");
        cqlSession.execute(CQL1);

        for (int C_W_ID = 1; C_W_ID <= 10; C_W_ID++) {
            for (int C_D_ID = 1; C_D_ID <= 10; C_D_ID++) {
                // CQL2
                String CQL2 = String.format("select C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from dbycql.customer_balance where C_W_ID = %d and C_D_ID = %d limit 10;", C_W_ID, C_D_ID);
                rs = cqlSession.execute(CQL2);
                rows = rs.all();
                for (Row row : rows) {
//                    int C_W_ID = row.getInt(0);
//                    int C_D_ID = row.getInt(1);
                    int C_ID = row.getInt(2);
                    String C_FIRST = row.getString(3);
                    String C_MIDDLE = row.getString(4);
                    String C_LAST = row.getString(5);
                    BigDecimal C_BALANCE = row.getBigDecimal(6);

                    // CQL3
                    String CQL3 = String.format("insert into dbycql.customer_balance_top10 (CB_TOP10, CB_W_ID, CB_D_ID, CB_ID, CB_FIRST, CB_MIDDLE, CB_LAST, CB_BALANCE, CB_TIME) values ('top10', %d, %d, %d, %s, %s, %s, %f now());", "top10", C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, System.currentTimeMillis());
                    // TODO: top10?
                    cqlSession.execute(CQL3);
                }
            }
        }
        // CQL4
        String CQL4 = String.format("select CB_W_ID, CB_D_ID, CB_ID, CB_FIRST, CB_MIDDLE, CB_LAST, CB_BALANCE from dbycql.customer_balance_top10 limit 10;");
        rs = cqlSession.execute(CQL4);
        rows = rs.all();
        for (Row row : rows) {
            int C_W_ID = row.getInt(0);
            int C_D_ID = row.getInt(1);
            int C_ID = row.getInt(2);
            String C_FIRST = row.getString(3);
            String C_MIDDLE = row.getString(4);
            String C_LAST = row.getString(5);
            BigDecimal C_BALANCE = row.getBigDecimal(6);

            // CQL5
            String CQL5 = String.format("select W_NAME from dbycql.Warehouse where W_ID = %d;", C_W_ID);
            rs = cqlSession.execute(CQL5);
            String W_NAME = rs.one().getString(0);

            // CQL6
            String CQL6 = String.format("select D_NAME from dbycql.District where D_W_ID = %d and D_ID = %d;", C_W_ID, C_D_ID);
            rs = cqlSession.execute(CQL6);
            String D_NAME = rs.one().getString(0);
            System.out.printf("C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s,C_BALANCE=%f,W_NAME=%s,D_NAME=%s\n", C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, W_NAME, D_NAME);
        }
        // CQL7
        String CQL7 = String.format("DROP TABLE dbycql.customer_balance_top10;");
        // TODO
        // Exception in thread "main" com.datastax.oss.driver.api.core.servererrors.InvalidQueryException: Duplicate Object. Object 'dbycql.customer_balance_top10' already exists
        cqlSession.execute(CQL7);
    }
}
