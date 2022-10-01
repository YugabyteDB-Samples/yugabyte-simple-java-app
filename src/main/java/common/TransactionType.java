package common;

/**
 * @Package common
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 1/10/22 4:03 PM
 */
public enum TransactionType {
    NEW_ORDER("N", "New order transaction"),
    PAYMENT("P","Payment transaction"),
    DELIVERY("D", "Delivery transaction"),
    ORDER_STATUS("O", "Order status transaction"),
    STOCK_LEVEL("S", "Stock-level transaction"),
    POPULAR_ITEM("I", "Popular-item transaction"),
    TOP_BALANCE("T", "Top-balance transaction"),
    RELATED_CUSTOMER("R", "Related-customer transaction"),
    ;

    public String type;
    public String description;
    TransactionType(String type, String description) {
        this.type = type;
        this.description = description;
    }

    @Override
    public String toString() {
        return "TransactionType{" +
                "type='" + type + '\'' +
                '}';
    }
}
