package common.transactionImpl;

import common.SQLEnum;
import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:36 AM
 */
public class TopBalanceTransaction extends Transaction {
    @Override
    protected void actuallyExecute(Connection conn) {
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.TopBalanceTransaction1.SQL));
            while (rs.next()) {
                int C_FIRST = rs.getInt(1);
                int C_MIDDLE = rs.getInt(2);
                int C_LAST = rs.getInt(3);
                int C_BALANCE = rs.getInt(4);
                int W_NAME = rs.getInt(5);
                int D_NAME = rs.getInt(6);
                System.out.printf("C_FIRST=%d,C_MIDDLE=%d,C_LAST=%d,C_BALANCE=%d,W_NAME=%d,D_NAME=%d\n",C_FIRST,C_MIDDLE,C_LAST,C_BALANCE,W_NAME,D_NAME);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
