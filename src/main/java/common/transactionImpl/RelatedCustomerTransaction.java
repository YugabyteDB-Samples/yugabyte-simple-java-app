package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class RelatedCustomerTransaction extends Transaction {
    int C_W_ID;
    int C_D_ID;
    int C_ID;

    @Override
    protected void YCQLExecute(CqlSession session) {
        HashMap<List<Integer>, Integer> outputLine = new HashMap<List<Integer>, Integer>();
        SimpleStatement stmt = SimpleStatement.newInstance(String.format("select " +
                "O_W_ID, " +
                "O_D_ID, " +
                "O_ID " +
                "from order " +
                "where O_W_ID=%d AND O_D_ID=%d AND O_C_ID=%d " +
                "allow filtering", C_W_ID, C_D_ID, C_ID));
        com.datastax.oss.driver.api.core.cql.ResultSet rs = session.execute(stmt);
        for (Row row : rs) {
            StringBuilder itemList = new StringBuilder("(");
            stmt = SimpleStatement.newInstance(String.format("select " +
                    "OL_I_ID " +
                    "from orderline " +
                    "where OL_W_ID=%d " +
                    "and OL_D_ID=%d" +
                    "and OL_O_ID=%d " +
                    "tallow filtering", row.getInt(0), row.getInt(1), row.getInt(2)));
            com.datastax.oss.driver.api.core.cql.ResultSet newRs = session.execute(stmt);
            int count_item = 0;
            for (Row newRow : newRs) {
                if (count_item != 0) {
                    itemList.append(",");
                }
                itemList.append(String.valueOf(newRow.getInt("0")));
                count_item++;
            }
            itemList.append(")");

            stmt = SimpleStatement.newInstance(String.format("select \n" +
                    "CI_C_ID, " +
                    "CI_W_ID, " +
                    "CI_D_ID, " +
                    "CI_O_ID, " +
                    "CI_I_ID " +
                    "from customer_item " +
                    "where CI_I_ID in %s " +
                    "and CI_W_ID!=%d " +
                    "allow filtering", itemList, C_W_ID));

        }
        // 存前三个ID作为key和对应出现item次数作为value
        com.datastax.oss.driver.api.core.cql.ResultSet outRs = session.execute(stmt);
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
        // 拿到了outputLine作为一个以List为key，MutableInteger为value的hashMap，后面对这个解析输出即可
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
