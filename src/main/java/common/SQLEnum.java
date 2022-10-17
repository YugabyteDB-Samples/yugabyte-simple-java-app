package common;

import common.transactionImpl.OrderStatusTransaction;

/**
 * @Package common
 * @Author Zhang Yang
 * @Version V1.0
 * @Date 10/10/22 10:52 AM
 */
public enum SQLEnum {
    OrderStatusTransaction1("select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from Customer where C_W_ID = ? and C_D_ID = ? and C_ID = ?",
            "",
            "select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from Customer where C_W_ID = 'C_W_ID' and C_D_ID = 'C_D_ID' and C_ID = 'C_ID'"),

    OrderStatusTransaction2("select O_ID, O_ENTRY_D, O_CARRIER_ID from Orders where O_W_ID = ? and O_D_ID = ? and O_C_ID = ? order by O_ID desc limit 1",
            "",
            "select O_ID, O_ENTRY_D, O_CARRIER_ID from Orders where O_W_ID = 'C_W_ID' and O_D_ID = 'C_D_ID' and O_C_ID = 'C_ID' order by O_ID desc limit 1"),

    OrderStatusTransaction3("select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from OrderLine where OL_W_ID = ? and OL_D_ID = ? and OL_O_ID = ?"
            ,""
            ,"select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from OrderLine where OL_W_ID = 'C_W_ID' OL_D_ID = 'C_D_ID' OL_O_ID = 'O_ID'"),

    StockLevelTransaction1("select D_NEXT_O_ID from District where D_W_ID = %d and D_ID = %d",
            "",
            "select D_NEXT_O_ID from District where D_W_ID = 'W_ID' and D_ID = 'D_ID'"),

    StockLevelTransaction2("with last_l_ol_orders as( select * from OrderLine where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID >= %d - %d and OL_O_ID < %d ) select count(distinct S_I_ID) as item_cnt from last_l_ol_orders t1 left join Stock t2 on t1.OL_W_ID = t2.S_W_ID and t1.OL_I_ID = t2.S_I_ID where S_QUANTITY < %d",
            "",
            "with last_l_ol_orders as( select * from OrderLine where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID >= 'N'-'L' and OL_O_ID < 'N' ) select count(distinct S_I_ID) as item_cnt from last_l_ol_orders t1 left join Stock t2 on t1.OL_W_ID = t2.S_W_ID and t1.OL_I_ID = t2.S_I_ID where S_QUANTITY < 'T'"),

    PopularItemTransaction1("with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank_order from Orders where O_W_ID = %d and O_D_ID = %d ) t where rank_order <= %d ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank_item from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t1.O_ENTRY_D, t2.C_FIRST, t2.C_MIDDLE, t2.C_LAST from last_l_orders t1 left join Customer t2 on t1.O_W_ID = t2.C_W_ID and t1.O_D_ID = t2.C_D_ID and t1.O_ID = t2.C_ID",
            "",
            "with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank_order from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' ) t where rank_order <= 'L' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank_item from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t1.O_ENTRY_D, t2.C_FIRST, t2.C_MIDDLE, t2.C_LAST from last_l_orders t1 left join Customer t2 on t1.O_W_ID = t2.C_W_ID and t1.O_D_ID = t2.C_D_ID and t1.O_ID = t2.C_ID ;"),
    PopularItemTransaction2("with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank_order from Orders where O_W_ID = %d and O_D_ID = %d ) t where rank_order <= %d ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank_item from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank_item = 1 order by t1.O_ID",
            "",
            "with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank_order from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' ) t where rank_order <= 'L' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank_item from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank_item = 1 order by t1.O_ID ;"),
    /*com.yugabyte.util.PSQLException: ERROR: column reference "rank" is ambiguous*/
    PopularItemTransaction3("with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank_order from Orders where O_W_ID = %d and O_D_ID = %d ) t where rank_order <= %d ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank_item from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t3.I_NAME, count(t2.OL_I_ID) / %d * 100 as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank_item = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t3.I_NAME",
            "",
            "with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank_order from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' ) t where rank_order <= 'L' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank_item from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t3.I_NAME, count(t2.OL_I_ID) / 'L' * 100 as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank_item = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t3.I_NAME"),
    PopularItemTransaction4("select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank = 1 order by t1.O_ID",
            "",
            "with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' ) t where rank <= 'L' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t1.O_ID, t2.I_NAME, t1.OL_QUANTITY from last_l_orders_items t1 left join Item t2 on t1.OL_I_ID = t2.I_ID where t1.rank = 1 order by t1.O_ID ;"),
    /*com.yugabyte.util.PSQLException: ERROR: relation "last_l_orders_items" does not exist
  位置：48
	at com.yugabyte.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2675)
	at com.yugabyte.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2365)
	at com.yugabyte.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:355)
*/
    PopularItemTransaction5("select t3.I_NAME, count(t2.OL_I_ID) / %d * 100 as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t2.I_NAME",
            "",
            "with last_l_orders as ( select * from (select *, row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank from Orders where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' ) t where rank <= 'L' ), last_l_orders_items as ( select *, rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank from last_l_orders t1 left join OrderLine t2 on t1.O_W_ID = t2.OL_W_ID and t1.O_D_ID = t2.OL_D_ID and t1.O_ID = t2.OL_O_ID ) select t3.I_NAME, count(t2.OL_I_ID) / 'L' * 100 as I_Percentage from (select distinct OL_I_ID from last_l_orders_items where rank = 1) t1 left join last_l_orders_items t2 on t1.OL_I_ID = t2.OL_I_ID left join Item t3 on t1.OL_I_ID = t3.I_ID group by t2.I_NAME"),

    TopBalanceTransaction1("with top_10_customers as( select * from Customer order by C_BALANCE desc limit 10 ) select t1.C_FIRST, t1.C_MIDDLE, t1.C_LAST, t1.C_BALANCE, t2.W_NAME, t3.D_NAME from top_10_customers t1 left join Warehouse t2 on t1.C_W_ID = t2.W_ID left join District t3 on t1.C_D_ID = t3.D_ID",
            "",
            "with top_10_customers as( select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, from Customer order by C_BALANCE desc limit 10 ) select t1.C_FIRST, t1.C_MIDDLE, t1.C_LAST, t1.C_BALANCE, t2.W_NAME, t3.D_NAME from top_10_customers t1 left join Warehouse t2 on t1.C_W_ID = t2.W_ID left join District t3 on t1.C_D_ID = t3.D_ID"),
    ;


    public String SQL;
    public String description;
    public String originalSQL;

    SQLEnum(String SQL, String description, String originalSQL) {
        this.SQL = SQL;
        this.description = description;
        this.originalSQL = originalSQL;
    }
}
