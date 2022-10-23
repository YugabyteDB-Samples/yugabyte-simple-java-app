package common.transactionImpl;

import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;

import java.sql.Connection;
import java.sql.SQLException;


public class PaymentTransaction extends Transaction {
    int C_W_ID;
    int C_D_ID;
    int C_ID;
    float PAYMENT;

    @Override
    protected void YSQLExecute(Connection conn) throws SQLException {
        //TODO : merge payment_cql
    }

    @Override
    protected void YCQLExecute(CqlSession cqlSession) {
        //TODO
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

    public float get_PAYMENT() {
        return PAYMENT;
    }

    public void set_PAYMENT(float payment) {
        PAYMENT = payment;
    }

    @Override
    public String toString() {
        return "PaymentTransaction{" +
                "C_W_ID=" + C_W_ID +
                ", C_D_ID=" + C_D_ID +
                ", C_ID=" + C_ID +
                "}\n";
    }
}
