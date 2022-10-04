package common.transactionImpl;

import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:36 AM
 */
public class TopBalanceTransaction extends Transaction {
    @Override
    protected void actuallyExecute(Connection conn) {
        String sql = String.format("with top_10_customers as( select C_FIRST, t1.C_MIDDLE, C_LAST, C_BALANCE, from( select *, row_number()over(order by C_BALANCE desc) as rank from Customer ) where rank <= 10 ) select t1.C_FIRST, t1.C_MIDDLE, t1.C_LAST, t1.C_BALANCE, t2.W_NAME, t3.D_NAME from top_10_customers t1 left join Warehouse t2 on t1.C_W_ID = t2.W_ID left join District t3 on t1.C_D_ID = t3.D_ID");
        try {
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                int C_FIRST = rs.getInt(1);
                int C_MIDDLE = rs.getInt(2);
                int C_LAST = rs.getInt(3);
                int C_BALANCE = rs.getInt(4);
                int W_NAME = rs.getInt(5);
                int D_NAME = rs.getInt(6);
                System.out.printf("%d,%d,%d,%d,%d,%d\n",C_FIRST,C_MIDDLE,C_LAST,C_BALANCE,W_NAME,D_NAME);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
