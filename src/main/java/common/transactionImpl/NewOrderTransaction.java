package common.transactionImpl;

import common.Transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    protected void YSQLExecute(Connection conn) {
        String sql1 = String.format("update District set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = %d and D_ID = %d",W_ID,D_ID);
        String sql2 = String.format("select D_NEXT_O_ID from District where D_W_ID = %d and D_ID = %d",W_ID,D_ID);
        String sql3 = String.format("create table new_order_info (NO_O_ID, NO_N, NO_W_ID, NO_D_ID, NO_C_ID, NO_I_ID, NO_SUPPLY_W_ID, NO_QUANTITY primary key (NO_O_ID, NO_N))");
//        try {
//            conn.createStatement().executeUpdate(sql1); // Update
//            ResultSet rs = conn.createStatement().executeQuery(sql2);
//            int N = -1;
//            while (rs.next()) {
//                N = rs.getInt(1);
//            }
//            conn.createStatement().executeUpdate(sql3);
//            for (int i = 0; i < items.size(); i++) {
//                int ITEM_NUMBER = items.get(i);
////                int ITEM_NUMBER = items.get(i);
//                String sql4 = String.format("insert into new_order_info values 'N', i, 'W_ID', 'D_ID', 'C_ID', 'OL_I_ID', 'OL_SUPPLY_W_ID', 'OL_QUANTITY'",N, )
//            }
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
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
