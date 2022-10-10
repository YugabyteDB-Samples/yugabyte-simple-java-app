package common.transactionImpl;

import common.Transaction;

import java.sql.Connection;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 2/10/22 11:06 AM
 */
public class OrderStatusTransaction extends Transaction {
    int C_W_ID;
    int C_D_ID;
    int C_ID;

//    @Override
//    protected void actuallyExecute(Connection conn) {
//        super.actuallyExecute(conn);
//    }

    public OrderStatusTransaction(int c_W_ID, int c_D_ID, int c_ID) {
        C_W_ID = c_W_ID;
        C_D_ID = c_D_ID;
        C_ID = c_ID;
    }

    public int getC_W_ID() {
        return C_W_ID;
    }

    public void setC_W_ID(int c_W_ID) {
        C_W_ID = c_W_ID;
    }

    public int getC_D_ID() {
        return C_D_ID;
    }

    public void setC_D_ID(int c_D_ID) {
        C_D_ID = c_D_ID;
    }

    public int getC_ID() {
        return C_ID;
    }

    public void setC_ID(int c_ID) {
        C_ID = c_ID;
    }
}
