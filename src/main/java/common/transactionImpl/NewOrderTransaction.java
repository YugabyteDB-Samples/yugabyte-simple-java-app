package common.transactionImpl;

import common.Transaction;

import java.sql.*;
import java.time.Instant;
import java.util.List;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 4:12 PM
 */
public class NewOrderTransaction extends Transaction {
    int W_ID;
    int D_ID;
    int C_ID;
    List<Integer> items;
    List<Integer> supplierWarehouses;
    List<Integer> quantities;

    @Override
    protected void YSQLExecute(Connection conn) throws SQLException {
        PreparedStatement statement = null;
        ResultSet rs = null;

        // Step 1,2
        // SQL1
        String SQL1 = "update District set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = ? and D_ID = ? returning D_NEXT_O_ID;";
//        String SQL1 = "update District set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = 'W_ID' and D_ID = 'D_ID' returning D_NEXT_O_ID;";
        statement = conn.prepareStatement(SQL1);
        statement.setInt(1, W_ID);
        statement.setInt(2, D_ID);
        rs = statement.executeQuery();
        int N = -1;
        while (rs.next()) {
            int D_NEXT_O_ID = rs.getInt(1);
            N = D_NEXT_O_ID - 1;
        }

        // SQL2
        String SQL2 = "create table new_order_info (NO_O_ID int NOT NULL, NO_N int NOT NULL, NO_W_ID int NOT NULL, NO_D_ID int NOT NULL, NO_C_ID int NOT NULL, NO_I_CNT int NOT NULL, NO_I_ID int NOT NULL, NO_SUPPLY_W_ID int NOT NULL, NO_QUANTITY decimal(2,0) NOT NULL, primary key (NO_O_ID, NO_N, NO_W_ID, NO_D_ID, NO_C_ID));";
        statement = conn.prepareStatement(SQL2);
        statement.executeUpdate();

        int NO_ALL_LOCAL = 1;
        int M = items.size();
        for (int i = 0; i < M; i++) {
            // SQL3
            int OL_I_ID = items.get(i);
            int OL_SUPPLY_W_ID = supplierWarehouses.get(i);
            int OL_QUANTITY = quantities.get(i);
            String SQL3 = "insert into new_order_info values (?, ?, ?, ?, ?, ?, ?, ?, ?);";
            statement = conn.prepareStatement(SQL3);
            statement.setInt(1, N);// first is 1
            statement.setInt(2, i);
            statement.setInt(3, W_ID);
            statement.setInt(4, D_ID);
            statement.setInt(5, C_ID);
            statement.setInt(6, M);
            statement.setInt(7, OL_I_ID);
            statement.setInt(8, OL_SUPPLY_W_ID);
            statement.setDouble(9, OL_QUANTITY);
            statement.executeUpdate();
            if (W_ID != OL_SUPPLY_W_ID) NO_ALL_LOCAL = 0;
        }

        // Step 3
        Timestamp current_time = Timestamp.from(Instant.now());
        String SQL4 = "insert into Orders (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL) values (?, ?, ?, ?, ?, NULL, ?, ?);";
        statement = conn.prepareStatement(SQL4);
        statement.setInt(1, N);
        statement.setInt(2, D_ID);
        statement.setInt(3, W_ID);
        statement.setInt(4, C_ID);
        statement.setTimestamp(5, current_time);
        statement.setInt(6, M);
        statement.setInt(7, NO_ALL_LOCAL);

        // Step 4
        double TOTAL_AMOUNT = 0;

        // step 5 - Stock
        String SQL5 = "update Stock set S_QUANTITY = ADJUSTED_QTY, S_YTD = NO_YTD, S_ORDER_CNT = NO_ORDER_CNT, S_REMOTE_CNT = NO_REMOTE_CNT from (select t1.NO_SUPPLY_W_ID, t1.NO_I_ID, case when t1.NO_QUANTITY - t2.S_QUANTITY < 10 then t1.NO_QUANTITY - t2.S_QUANTITY + 100 else t1.NO_QUANTITY - t2.S_QUANTITY end as ADJUSTED_QTY, t1.NO_QUANTITY as NO_YTD, t2.S_ORDER_CNT + 1 as NO_ORDER_CNT, S_REMOTE_CNT + case when t1.NO_SUPPLY_W_ID != t1.NO_W_ID then 1 else 0 end as NO_REMOTE_CNT from new_order_info t1 left join Stock t2 on t1.NO_SUPPLY_W_ID = t2.S_W_ID and t1.NO_I_ID = t2.S_I_ID) t where S_W_ID = t.NO_SUPPLY_W_ID and S_I_ID = t.NO_I_ID;";
        statement = conn.prepareStatement(SQL5);
        statement.executeUpdate();

        // step 5 - OrderLine
        String SQL6 = "insert into OrderLine select NO_W_ID, NO_D_ID, NO_O_ID, NO_N, NO_I_ID, NULL, NO_QUANTITY * I_PRICE as ITEM_AMOUNT, NO_SUPPLY_W_ID, NO_QUANTITY, CONCAT(?, D_ID) as NO_DIST_INFO from new_order_info t1 left join District t2 on t1.NO_W_ID = t2.D_W_ID and t1.NO_D_ID = t2.D_ID left join Item t3 on t1.NO_I_ID = t3.I_ID;";
        statement = conn.prepareStatement(SQL6);
        statement.executeUpdate();

        // step 6
        String SQL7 = "select sum(OL_AMOUNT) as TOTAL_AMOUNT from OrderLine where OL_O_ID = ? and OL_D_ID = ? and OL_W_ID = ?;";
        statement = conn.prepareStatement(SQL7);
        statement.setInt(1, N);
        statement.setInt(2, D_ID);
        statement.setInt(3, W_ID);
        rs = statement.executeQuery();
        while (rs.next()) {
            TOTAL_AMOUNT = rs.getDouble(1); // starts from 1
        }

        String SQL8 = "select W_TAX, D_TAX, C_LAST, C_CREDIT, C_DISCOUNT from Warehouse t1 left join District t2 on t1.W_ID = t2.D_W_ID left join Customer t3 on t2.D_W_ID = t3.C_W_ID and t2.D_ID = t3.C_D_ID where D_W_ID = ? and D_ID = ? and C_ID = ?;";
        statement = conn.prepareStatement(SQL8);
        statement.setInt(1, W_ID);
        statement.setInt(2, D_ID);
        statement.setInt(3, C_ID);
        rs = statement.executeQuery();
        while (rs.next()) {
            double W_TAX = rs.getDouble(1);
            double D_TAX = rs.getDouble(2);
            String C_LAST = rs.getString(3);
            String C_CREDIT = rs.getString(4);
            double C_DISCOUNT = rs.getDouble(5);
            TOTAL_AMOUNT = TOTAL_AMOUNT * (1 + D_TAX + W_TAX) * (1 - C_DISCOUNT);

            System.out.printf("W_ID=%d,D_ID=%d,C_ID=%d,C_LAST=%s,C_CREDIT=%s,C_DISCOUNT=%s,W_TAX=%f,D_TAX=%f,N=%d,current_time=%s,M=%d,TOTAL_AMOUNT=%f\n"
                    , W_ID, D_ID, C_ID, C_LAST, C_CREDIT, C_DISCOUNT, W_TAX, D_TAX, N, current_time, M, TOTAL_AMOUNT);
        }

        String SQL9 = "select NO_I_ID, I_NAME, NO_SUPPLY_W_ID, NO_QUANTITY, NO_QUANTITY * I_PRICE as OL_AMOUNT, S_QUANTITY from new_order_info t1 left join Item t2 on t1.NO_I_ID = t2.I_ID left join Stock t3 on t1.NO_I_ID = t3.S_I_ID and t1.NO_SUPPLY_W_ID = t3.S_W_ID;";
        statement = conn.prepareStatement(SQL9);
        rs = statement.executeQuery();
        while (rs.next()) {
            int NO_I_ID = rs.getInt(1);
            String I_NAME = rs.getString(2);
            int NO_SUPPLY_W_ID = rs.getInt(3);
            int NO_QUANTITY = rs.getInt(4);
            double OL_AMOUNT = rs.getDouble(5);
            double S_QUANTITY = rs.getDouble(6);
            System.out.printf("NO_I_ID=%d,I_NAME=%s,NO_SUPPLY_W_ID=%d,NO_QUANTITY=%d,OL_AMOUNT=%f,S_QUANTITY=%f\n",
                    NO_I_ID,I_NAME,NO_SUPPLY_W_ID,NO_QUANTITY,OL_AMOUNT,S_QUANTITY);
        }

        String SQL10 = "insert into customer_item select NO_W_ID, NO_D_ID, NO_C_ID, NO_O_ID, NO_I_ID, NO_N from new_order_info;";
        statement = conn.prepareStatement(SQL10);
        statement.executeUpdate();

        String SQL11 = "drop table new_order_info;";
        statement = conn.prepareStatement(SQL11);
        statement.executeUpdate();
    }

    public int getW_ID() {
        return W_ID;
    }

    public void setW_ID(int w_ID) {
        W_ID = w_ID;
    }

    public int getD_ID() {
        return D_ID;
    }

    public void setD_ID(int d_ID) {
        D_ID = d_ID;
    }

    public int getC_ID() {
        return C_ID;
    }

    public void setC_ID(int c_ID) {
        C_ID = c_ID;
    }

    public List<Integer> getSupplierWarehouses() {
        return supplierWarehouses;
    }

    public void setSupplierWarehouses(List<Integer> supplierWarehouses) {
        this.supplierWarehouses = supplierWarehouses;
    }

    public List<Integer> getQuantities() {
        return quantities;
    }

    public void setQuantities(List<Integer> quantities) {
        this.quantities = quantities;
    }

    public List<Integer> getItems() {
        return items;
    }

    public void setItems(List<Integer> items) {
        this.items = items;
    }

    @Override
    public String toString() {
        return "NewOrderTransaction{" +
                "W_ID=" + W_ID +
                ", D_ID=" + D_ID +
                ", C_ID=" + C_ID +
                ", items=" + items +
                ", supplierWarehouses=" + supplierWarehouses +
                ", quantities=" + quantities +
                "}\n";
    }
}
