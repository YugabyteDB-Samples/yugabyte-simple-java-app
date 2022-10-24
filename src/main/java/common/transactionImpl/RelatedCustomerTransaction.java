package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.*;

public class RelatedCustomerTransaction extends Transaction {
    int C_W_ID;
    int C_D_ID;
    int C_ID;

    protected void YSQLExecute(Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.executeQuery(String.format("with target_orderline as(" +
                "select " +
                "* from customer_item " +
                "where CI_W_ID=%d AND CI_D_ID=%d AND CI_C_ID=%d), " +
                "other_orderline as(" +
                "select " +
                "* from customer_item " +
                "where CI_W_ID!=%d) " +
                "select " +
                "target_w_id,target_d_id,target_c_id, " +
                "output_w_id,output_d_id,output_c_id " +
                "from (" +
                "select " +
                "t1.CI_W_ID as target_w_id,t1.CI_D_ID as target_d_id,t1.CI_C_ID as target_c_id, " +
                "t2.CI_W_ID as output_w_id,t2.CI_D_ID as output_d_id,t2.CI_C_ID as output_c_id, " +
                "count(*) as common_cnt " +
                "from " +
                "target_orderline as t1 " +
                "left join other_orderline as t2 " +
                "on t1.CI_I_ID=t2.CI_I_ID " +
                "group by t1.CI_W_ID, t1.CI_D_ID, t1.CI_C_ID, " +
                "t2.CI_W_ID, t2.CI_D_ID, t2.CI_C_ID " +
                ")a " +
                "where a.common_cnt>=2", C_W_ID, C_D_ID, C_ID, C_W_ID));
    }


    protected void YCQLExecute(CqlSession session) {
        System.out.println("执行related cql中..");
        HashMap<List<Integer>, Integer> outputLine = new HashMap<List<Integer>, Integer>();
        SimpleStatement stmt = SimpleStatement.newInstance(String.format("select " +
                "O_W_ID, " +
                "O_D_ID, " +
                "O_ID " +
                "from dbycql.orders2 " +
                "where O_W_ID=%d AND O_D_ID=%d AND O_C_ID=%d ", C_W_ID, C_D_ID, C_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs = session.execute(stmt);
        for (Row row : rs) {
            StringBuilder itemList = new StringBuilder("(");
            stmt = SimpleStatement.newInstance(String.format("select " +
                    "OL_I_ID " +
                    "from dbycql.orderline2 " +
                    "where OL_W_ID=%d " +
                    "and OL_D_ID=%d" +
                    "and OL_O_ID=%d ", row.getInt(0), row.getInt(1), row.getInt(2)));
            com.datastax.oss.driver.api.core.cql.ResultSet newRs = session.execute(stmt);
            int count_item = 0;
            for (Row newRow : newRs) {
                if (count_item != 0) {
                    itemList.append(",");
                }
                itemList.append(String.valueOf(newRow.getInt("OL_I_ID")));
                count_item++;
            }
            itemList.append(")");
            // 针对最后一个超时问题设置oltp格式的stmt
            String last_cql = String.format("select " +
                    "CI_C_ID, " +
                    "CI_W_ID, " +
                    "CI_D_ID, " +
                    "CI_O_ID, " +
                    "CI_I_ID " +
                    "from dbycql.customer_item " +
                    "where CI_I_ID in %s " +
                    "and CI_W_ID != %d ", itemList, C_W_ID);
            // 存前三个ID作为key和对应出现item次数作为value
            SimpleStatement simpleStatement = SimpleStatement.builder(last_cql)
                    .setExecutionProfileName("oltp").setTimeout(Duration.ofSeconds(30))
                    .build();
            com.datastax.oss.driver.api.core.cql.ResultSet outRs = session.execute(simpleStatement);
            for (Row finalRs : outRs) {
                if (!Objects.equals(String.valueOf(finalRs.getInt("CI_W_ID")), String.valueOf(C_W_ID))) {
                    List<Integer> order_info = Arrays.asList(finalRs.getInt("CI_W_ID"), finalRs.getInt("CI_D_ID"), finalRs.getInt("CI_C_ID"));
                    boolean flag = outputLine.containsKey(order_info);
                    if (flag) {
                        outputLine.put(order_info, outputLine.get(order_info) + 1);
                    } else {
                        outputLine.put(order_info, 1);
                    }
                }
            }
            System.out.println(outputLine);
            // 拿到了outputLine作为一个以List为key，MutableInteger为value的hashMap，后面对这个解析输出即可
        }
    }

    public int getC_W_ID() {
        return C_W_ID;
    }

    public void setC_W_ID(int c_w_ID) {
        C_W_ID = c_w_ID;
    }

    public int getC_D_ID() {
        return C_D_ID;
    }

    public void setC_D_ID(int c_d_ID) {
        C_D_ID = c_d_ID;
    }

    public int getC_ID() {
        return C_ID;
    }

    public void setC_ID(int c_ID) {
        C_ID = c_ID;
    }
}
