package common.transactionImpl;

import common.Transaction;

import java.util.List;

/**
 * @Package common.transactionImpl
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 4:12 PM
 */
public class NewOrderTransaction extends Transaction{
    // define template method for transaction execution
    int W_ID;
    int D_ID;
    int C_ID;
    int numberOfItems;
    List<Integer> itemNumbers;
    List<Integer> supplierWarehouses;
    List<Integer> quantities;

    @Override
    protected void actuallyExecute() {
    }


}
