package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.Iterator;

public class DeliveryTransaction extends Transaction {
    int W_ID;
    int CARRIER_ID;
    protected void YSQLExecute(Connection conn) {
        try {
            Statement stmt = conn.createStatement();
            // 获取到每个district对应的最小order_number
            ResultSet rs = stmt.executeQuery(String.format("select " +
                    "t1.O_W_ID, t1.O_D_ID, t1.O_ID, t1.O_C_ID, t2.SUM_AMT " +
                    "from(" +
                    "select " +
                    "O_W_ID, O_D_ID, O_ID, O_C_ID," +
                    "rank() over(partition by O_W_ID,O_D_ID order by O_ID ASC) as rnk " +
                    "FROM Orders WHERE O_W_ID=%d and O_CARRIER_ID is null" +
                    ")t1 " +
                    "left join " +
                    "(SELECT " +
                    "OL_W_ID, OL_D_ID, OL_O_ID," +
                    "SUM(OL_AMOUNT) AS SUM_AMT " +
                    "FROM " +
                    "orderline " +
                    "GROUP BY " +
                    "OL_W_ID, OL_D_ID, OL_O_ID" +
                    ")t2 " +
                    "on t1.O_W_ID=t2.OL_W_ID AND t1.O_D_ID=t2.OL_D_ID AND t1.O_ID=t2.OL_O_ID " +
                    "where t1.rnk=1", W_ID));
            int[][] tmpList = new int[10][5];
            int index = 0;
            while (rs.next()) {
                System.out.println("存参数中...");
                tmpList[index][0] = rs.getInt(1);
                tmpList[index][1] = rs.getInt(2);
                tmpList[index][2] = rs.getInt(3);
                tmpList[index][3] = rs.getInt(4);
                tmpList[index][4] = rs.getInt(5);
                index++;
            }
            System.out.println("Delivery Transaction正在执行中...");
            // 写一个for循环
            for (int i = 0; i < 10; i++) {
                stmt.execute((String.format("UPDATE Orders SET O_CARRIER_ID=%d WHERE O_W_ID=%d and O_D_ID=%d and O_ID=%d", CARRIER_ID, tmpList[i][0], tmpList[i][1], tmpList[i][2])));
                stmt.execute(String.format("UPDATE OrderLine SET OL_DELIVERY_D=(SELECT CURRENT_TIMESTAMP) WHERE OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d", tmpList[i][0], tmpList[i][1], tmpList[i][2]));
                stmt.execute(String.format("UPDATE Customer SET C_BALANCE=C_BALANCE+%d WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", tmpList[i][4], tmpList[i][0], tmpList[i][1], tmpList[i][3]));
                stmt.execute(String.format("UPDATE Customer SET C_DELIVERY_CNT=C_DELIVERY_CNT+%d WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", 1, tmpList[i][0], tmpList[i][1], tmpList[i][2]));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    protected void cqlExecute(CqlSession session) {
        int o_ID = 0;
        int c_ID = 0;
        int max_Order = 0;
        int sum_Amt = 0;
        for (int d_ID = 1;  d_ID <= 10; d_ID ++) {
            // 第一个cql
            SimpleStatement stmt = SimpleStatement.newInstance(String.format("select" +
                    "O_ID," +
                    "O_C_ID " +
                    "from order_dev " +
                    "where WHERE O_W_ID=%d and O_CARRIER_ID is null and O_D_ID=%d" +
                    "LIMIT 1 allow filtering", W_ID, d_ID));
            com.datastax.oss.driver.api.core.cql.ResultSet rs = session.execute(stmt);
            Iterator<Row> rsIterator = rs.iterator();
            while (rsIterator.hasNext()) {
                Row row = rsIterator.next();
                o_ID = row.getInt(1);
                c_ID = row.getInt(2);
            }
            stmt = SimpleStatement.newInstance(String.format("UPDATE Order SET O_CARRIER_ID=%d " +
                    "WHERE O_W_ID=%d and O_D_ID=%d and O_ID=%d", CARRIER_ID, W_ID, d_ID, o_ID));
            session.execute(stmt);
            // 第二个cql
            stmt = SimpleStatement.newInstance(String.format("select max(OL_NUMBER) as max_order " +
                    "from OrderLine " +
                    "where OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d", W_ID, d_ID, o_ID));
            rs = session.execute(stmt);
            rsIterator = rs.iterator();
            while (rsIterator.hasNext()) {
                Row row = rsIterator.next();
                max_Order = row.getInt(1);
            }
            // 第三个cql
            for (int ol_num = 1; ol_num < max_Order; ol_num++) {
                stmt = SimpleStatement.newInstance(String.format("UPDATE OrderLine SET OL_DELIVERY_D=(SELECT CURRENT_TIMESTAMP) " +
                        "WHERE OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d and OL_NUMBER=%d", W_ID, d_ID, o_ID, ol_num));
                session.execute(stmt);
            }
            // 第四个cql
            stmt = SimpleStatement.newInstance(String.format("SELECT " +
                    "SUM(OL_AMOUNT) AS SUM_AMT " +
                    "FROM OrderLine " +
                    "WHERE OL_W_ID=%d and OL_D_ID=%d and OL_O_ID=%d " +
                    "allow filtering", W_ID, d_ID, o_ID));
            rs = session.execute(stmt);
            rsIterator = rs.iterator();
            while (rsIterator.hasNext()) {
                Row row = rsIterator.next();
                sum_Amt = row.getInt(1);
            }
            // 第五个cql
            stmt = SimpleStatement.newInstance(String.format("UPDATE Customer SET C_BALANCE=C_BALANCE+%d " +
                    "WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", sum_Amt, W_ID, d_ID, c_ID));
            session.execute(stmt);
            // 第六个cql
            stmt = SimpleStatement.newInstance(String.format("UPDATE Customer SET C_DELIVERY_CNT=C_DELIVERY_CNT+%d" +
                    "WHERE C_W_ID=%d and C_D_ID=%d and C_ID=%d", 1, W_ID, d_ID, c_ID));
        }
    }

    public int getW_ID() {
        return W_ID;
    }

    public void setW_ID(int w_ID) {
        W_ID = w_ID;
    }

    public int getCARRIER_ID() {
        return CARRIER_ID;
    }

    public void setCARRIER_ID(int carrier_ID) {
        CARRIER_ID = carrier_ID;
    }
}
