import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;
import common.transactionImpl.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class SampleApp {
    private static Connection conn;
    private static CqlSession cqlSession;

    public static void main(String[] args) {
        // 1. Construct requests from files.
        List<Transaction> list = null;
        try {
            list = readFile();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (list == null) throw new RuntimeException("Input list is null! Please check input files");

        // 2. Establish a DB connection
        try {
            if (DataSource.MODE.equals(DataSource.YSQL)) {
                System.out.println("Connecting to DB. Your mode is YSQL.");
                conn = DataSource.getSQLConnection();
                System.out.println("Isolation level=" + conn.getTransactionIsolation());
            } else {
                System.out.println("Connecting to DB. Your mode is YCQL.");
                cqlSession = DataSource.getCQLSession();
            }
            System.out.println(">>>> Successfully connected to YugabyteDB.");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 3. execute and report
        ExecuteManager executeManager = new ExecuteManager();
        if (DataSource.MODE.equals(DataSource.YSQL)) {
            try {
                executeManager.executeYSQL(conn, list);
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        } else {
            try {
                executeManager.executeYCQL(cqlSession, list);
            }
            finally {
                cqlSession.close();
            }
        }
        executeManager.report();
    }


    private static List<Transaction> readFile() throws FileNotFoundException {
        String inputFileName = "src/main/resources/xact_files/2.txt";
        Scanner scanner = new Scanner(new File(inputFileName));
        List<Transaction> list = new ArrayList<>();
        while (scanner.hasNextLine()) {
            String[] firstLine = scanner.nextLine().split(",");
            String type = firstLine[0];
            Transaction transaction = null;
            if (type.equals(TransactionType.PAYMENT.type)) {
                transaction = assemblePaymentTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.DELIVERY.type)) {
                transaction = assembleDeliveryTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.NEW_ORDER.type)) {
                transaction = assembleNewOrderTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.ORDER_STATUS.type)) {
                transaction = assembleOrderStatusTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.STOCK_LEVEL.type)) {
                transaction = assembleStockLevelTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.POPULAR_ITEM.type)) {
                transaction = assemblePopularItemTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.TOP_BALANCE.type)) {
                transaction = assembleTopBalanceTransaction(firstLine, scanner);
            } else if (type.equals(TransactionType.RELATED_CUSTOMER.type)) {
                transaction = assembleRelatedCustomerTransaction(firstLine, scanner);
            }
            if (transaction != null) list.add(transaction);
        }
        System.out.printf("Read {%d} orders from file={%s}\n", list.size(), inputFileName);
        return list;
    }

    private static Transaction assembleRelatedCustomerTransaction(String[] firstLine, Scanner scanner) {
        int C_W_ID = Integer.parseInt(firstLine[1]);
        int C_D_ID = Integer.parseInt(firstLine[2]);
        int C_ID = Integer.parseInt(firstLine[3]);
        RelatedCustomerTransaction relatedCustomerTransaction = new RelatedCustomerTransaction();
        relatedCustomerTransaction.setTransactionType(TransactionType.RELATED_CUSTOMER);
        relatedCustomerTransaction.setC_W_ID(C_W_ID);
        relatedCustomerTransaction.setC_D_ID(C_D_ID);
        relatedCustomerTransaction.setC_ID(C_ID);
        return relatedCustomerTransaction;
    }

    private static Transaction assembleTopBalanceTransaction(String[] firstLine, Scanner scanner) {
        TopBalanceTransaction topBalanceTransaction = new TopBalanceTransaction();
        topBalanceTransaction.setTransactionType(TransactionType.TOP_BALANCE);
//        System.out.println("add a top balance item trans");
        return topBalanceTransaction;
    }

    private static Transaction assemblePopularItemTransaction(String[] firstLine, Scanner scanner) {
        int W_ID = Integer.parseInt(firstLine[1]);
        int D_ID = Integer.parseInt(firstLine[2]);
        int L = Integer.parseInt(firstLine[3]);
        PopularItemTransaction popularItemTransaction = new PopularItemTransaction(W_ID, D_ID, L);
        popularItemTransaction.setTransactionType(TransactionType.POPULAR_ITEM);
//        System.out.println("add a popular item trans");
        return popularItemTransaction;
    }

    private static Transaction assembleStockLevelTransaction(String[] firstLine, Scanner scanner) {
        int W_ID = Integer.parseInt(firstLine[1]);
        int D_ID = Integer.parseInt(firstLine[2]);
        int T = Integer.parseInt(firstLine[3]);
        int L = Integer.parseInt(firstLine[4]);
        StockLevelTransaction stockLevelTransaction = new StockLevelTransaction(W_ID, D_ID, T, L);
        stockLevelTransaction.setTransactionType(TransactionType.STOCK_LEVEL);
//        System.out.println("add a stock level trans");
        return stockLevelTransaction;
    }

    private static Transaction assembleOrderStatusTransaction(String[] firstLine, Scanner scanner) {
        int C_W_ID = Integer.parseInt(firstLine[1]);
        int C_D_ID = Integer.parseInt(firstLine[2]);
        int C_ID = Integer.parseInt(firstLine[3]);
        OrderStatusTransaction orderStatusTransaction = new OrderStatusTransaction(C_W_ID, C_D_ID, C_ID);
        orderStatusTransaction.setTransactionType(TransactionType.ORDER_STATUS);
//        System.out.println("add a order status trans");
        return orderStatusTransaction;
    }

    /*
    New Order Transaction consists of M+1 lines, where M denote the number of items in the new order.
    The first line consists of five comma-separated values: N,C ID,W ID,D ID,M.
    Each of the M remaining lines specifies an item in the order and consists of three comma- separated values: OL I ID,OL SUPPLY W ID,OL QUANTITY.
     */
    private static Transaction assembleNewOrderTransaction(String[] firstLine, Scanner scanner) {
        NewOrderTransaction newOrderTransaction = new NewOrderTransaction();
        int C_ID = Integer.parseInt(firstLine[1]);
        int W_ID = Integer.parseInt(firstLine[2]);
        int D_ID = Integer.parseInt(firstLine[3]);
        int M = Integer.parseInt(firstLine[4]);
        List<Integer> items = new ArrayList<>();
        List<Integer> suppliers = new ArrayList<>();
        List<Integer> quanties = new ArrayList<>();
        for (int i = 0; i < M; i++) {
            String[] strs = scanner.nextLine().split(",");
            int OL_I_ID = Integer.parseInt(strs[0]);
            int OL_SUPPLY_W_ID = Integer.parseInt(strs[1]);
            int OL_QUANTITY = Integer.parseInt(strs[2]);
            items.add(OL_I_ID);
            suppliers.add(OL_SUPPLY_W_ID);
            quanties.add(OL_QUANTITY);
        }
        newOrderTransaction.setTransactionType(TransactionType.NEW_ORDER);
        newOrderTransaction.setC_ID(C_ID);
        newOrderTransaction.setD_ID(D_ID);
        newOrderTransaction.setW_ID(W_ID);
        newOrderTransaction.setItems(items);
        newOrderTransaction.setQuantities(quanties);
        newOrderTransaction.setSupplierWarehouses(suppliers);
        return newOrderTransaction;
    }

    private static Transaction assembleDeliveryTransaction(String[] firstLine, Scanner scanner) {
        DeliveryTransaction deliveryTransaction = new DeliveryTransaction();
        int W_ID = Integer.parseInt(firstLine[1]);
        int CARRIER_ID = Integer.parseInt(firstLine[2]);
        deliveryTransaction.setTransactionType(TransactionType.DELIVERY);
        deliveryTransaction.setW_ID(W_ID);
        deliveryTransaction.setCARRIER_ID(CARRIER_ID);
        return deliveryTransaction;
    }

    private static Transaction assemblePaymentTransaction(String[] firstLine, Scanner scanner) {
        PaymentTransaction paymentTransaction = new PaymentTransaction();
        int C_W_ID = Integer.parseInt(firstLine[1]);
        int C_D_ID = Integer.parseInt(firstLine[2]);
        int C_ID = Integer.parseInt(firstLine[3]);
        float PAYMENT = Float.parseFloat(firstLine[4]);
        paymentTransaction.setTransactionType(TransactionType.PAYMENT);
        paymentTransaction.setC_ID(C_ID);
        paymentTransaction.setC_D_ID(C_D_ID);
        paymentTransaction.setC_W_ID(C_W_ID);
        paymentTransaction.set_PAYMENT(PAYMENT);
        return paymentTransaction;
    }
}
