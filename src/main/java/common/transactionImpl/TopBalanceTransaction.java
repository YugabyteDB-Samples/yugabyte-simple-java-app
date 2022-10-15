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
    protected void YSQLExecute(Connection conn) {
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SQLEnum.TopBalanceTransaction1.SQL));
            while (rs.next()) {
                String C_FIRST = rs.getString(1);
                String C_MIDDLE = rs.getString(2);
                String C_LAST = rs.getString(3);
                double C_BALANCE = rs.getDouble(4);
                String W_NAME = rs.getString(5);
                String D_NAME = rs.getString(6);
                System.out.printf("C_FIRST=%s,C_MIDDLE=%s,C_LAST=%s,C_BALANCE=%f,W_NAME=%s,D_NAME=%s\n",C_FIRST,C_MIDDLE,C_LAST,C_BALANCE,W_NAME,D_NAME);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
