package common.transactionImpl;

import common.Transaction;

import java.sql.Connection;

public class RelatedCustomerTransaction extends Transaction {
    int C_W_ID;
    int C_D_ID;
    int C_ID;

//     @Override
//     protected void actuallyExecute(Connection conn) {

//     }

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
