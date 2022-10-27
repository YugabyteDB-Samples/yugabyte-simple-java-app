import com.datastax.oss.driver.api.core.CqlSession;
import common.Transaction;
import common.TransactionType;
import common.transactionImpl.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.Level;

public class SampleApp {
    private Connection conn;
    private CqlSession cqlSession;
    private static final int N = 20;
    private static int countDownLatchTimeout = 8;

    public static void main(String[] args) {
        String MODE = DataSource.YSQL;// by default, run YSQL
        if (args != null && args.length != 0 && args[0].equals(DataSource.YCQL)) MODE = DataSource.YCQL;
        String[] inputFileList = new String[N];
        String[] outputFileList = new String[N];
        for (int i = 0; i < N; i++) {
            inputFileList[i] = "src/main/resources/xact_files/" + i + ".txt";
            outputFileList[i] = "log" + i + ".txt";
            Logger logger = Logger.getLogger(outputFileList[i]);
            try {
                Handler handler = new FileHandler(outputFileList[i]);
                handler.setFormatter(new SimpleFormatter());
                logger.addHandler(handler);
                logger.setLevel(Level.FINEST);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        CountDownLatch countDownLatch = new CountDownLatch(N);
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

        for (int i = 0; i < N; i++) {
            String finalMODE = MODE;
            int finalI = i;
            cachedThreadPool.execute(() -> {
                Logger logger = Logger.getLogger(outputFileList[finalI]);
                try {
                    logger.log(Level.INFO, Thread.currentThread().getName() + " starts ");
                    new SampleApp().doWork(finalMODE, inputFileList[finalI], logger);
                } catch (Exception e) {
                    logger.log(Level.WARNING, Thread.currentThread().getName() + " exception ");
                } finally {
                    logger.log(Level.INFO, Thread.currentThread().getName() + " ends ");
                    countDownLatch.countDown();
                }
            });
        }

        System.out.println("Main thread waits");
        try {
            System.out.println("CountDownLatchTimeout = " + countDownLatchTimeout);
            countDownLatch.await(countDownLatchTimeout, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            System.out.println("Exception: await interrupted exception");
        } finally {
            System.out.println("countDownLatch: " + countDownLatch.toString());
        }
        System.out.println("Main thread ends");

        cachedThreadPool.shutdown();
    }

    public void doWork(String MODE, String inputFileName, Logger logger) {
        logger.log(Level.INFO, Thread.currentThread().getName() + "do work");

        // 1. Construct requests from files.
        List<Transaction> list = new ArrayList<>();
        try {
            readFile(inputFileName, list, logger);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (list == null) throw new RuntimeException("Input list is null! Please check input files");

        // 2. Establish a DB connection
        try {
            if (MODE.equals(DataSource.YSQL)) {
                logger.log(Level.INFO, Thread.currentThread().getName() + "do work");
                logger.log(Level.INFO, "Connecting to DB. Your mode is YSQL.");
                conn = new DataSource(MODE).getSQLConnection();
                logger.log(Level.INFO, "Conn = "+ conn.getClientInfo());
//                logger.log(Level.INFO, "Isolation level=" + conn.getTransactionIsolation());
            } else {
                logger.log(Level.INFO, "Connecting to DB. Your mode is YCQL.");
                cqlSession = new DataSource(MODE).getCQLSession();
                logger.log(Level.INFO, "CQLSession = "+ cqlSession.getName());
            }
            logger.log(Level.INFO, ">>>> Successfully connected to YugabyteDB.");
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // 3. execute and report
        ExecuteManager executeManager = new ExecuteManager();
        if (MODE.equals(DataSource.YSQL)) {
            try {
                executeManager.executeYSQL(conn, list, logger);
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
                executeManager.executeYCQL(cqlSession, list, logger);
            } finally {
                cqlSession.close();
            }
        }
        executeManager.report(logger);
    }


    private static List<Transaction> readFile(String fileName, List<Transaction> list, Logger logger) throws FileNotFoundException {
        logger.log(Level.INFO, Thread.currentThread().getName() + " reads from file " + fileName);
        Scanner scanner = new Scanner(new File(fileName));
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
        logger.log(Level.INFO, Thread.currentThread().getName() + " reads " + list.size() + " requests from " + fileName);
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
//        logger.log(Level.INFO, "add a top balance item trans");
        return topBalanceTransaction;
    }

    private static Transaction assemblePopularItemTransaction(String[] firstLine, Scanner scanner) {
        int W_ID = Integer.parseInt(firstLine[1]);
        int D_ID = Integer.parseInt(firstLine[2]);
        int L = Integer.parseInt(firstLine[3]);
        PopularItemTransaction popularItemTransaction = new PopularItemTransaction(W_ID, D_ID, L);
        popularItemTransaction.setTransactionType(TransactionType.POPULAR_ITEM);
//        logger.log(Level.INFO, "add a popular item trans");
        return popularItemTransaction;
    }

    private static Transaction assembleStockLevelTransaction(String[] firstLine, Scanner scanner) {
        int W_ID = Integer.parseInt(firstLine[1]);
        int D_ID = Integer.parseInt(firstLine[2]);
        int T = Integer.parseInt(firstLine[3]);
        int L = Integer.parseInt(firstLine[4]);
        StockLevelTransaction stockLevelTransaction = new StockLevelTransaction(W_ID, D_ID, T, L);
        stockLevelTransaction.setTransactionType(TransactionType.STOCK_LEVEL);
//        logger.log(Level.INFO, "add a stock level trans");
        return stockLevelTransaction;
    }

    private static Transaction assembleOrderStatusTransaction(String[] firstLine, Scanner scanner) {
        int C_W_ID = Integer.parseInt(firstLine[1]);
        int C_D_ID = Integer.parseInt(firstLine[2]);
        int C_ID = Integer.parseInt(firstLine[3]);
        OrderStatusTransaction orderStatusTransaction = new OrderStatusTransaction(C_W_ID, C_D_ID, C_ID);
        orderStatusTransaction.setTransactionType(TransactionType.ORDER_STATUS);
//        logger.log(Level.INFO, "add a order status trans");
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
