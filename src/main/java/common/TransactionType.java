package common;

/**
 * @Package common
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 4:03 PM
 */
public enum TransactionType {
    NEW_ORDER("N", "New order transaction",0),
    PAYMENT("P","Payment transaction",1),
    DELIVERY("D", "Delivery transaction",2),
    ORDER_STATUS("O", "Order status transaction",3),
    STOCK_LEVEL("S", "Stock-level transaction",4),
    POPULAR_ITEM("I", "Popular-item transaction",5),
    TOP_BALANCE("T", "Top-balance transaction",6),
    RELATED_CUSTOMER("R", "Related-customer transaction",7),
    ;

    public String type;
    public String description;
    public int index;
    TransactionType(String type, String description, int index) {
        this.type = type;
        this.index = index;
        this.description = description;
    }

    @Override
    public String toString() {
        return "TransactionType{" +
                "type='" + type + '\'' +
                '}';
    }
}
