package common.transactionImpl;

import common.Transaction;

import java.sql.Connection;

public class DeliveryTransaction extends Transaction {
    int W_ID;
    int CARRIER_ID;
//     protected void actuallyExecute(Connection conn) {
//         super.actuallyExecute(conn);
//     }

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
