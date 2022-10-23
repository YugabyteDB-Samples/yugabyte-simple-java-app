-- YSQL
-- 1. New Order Transaction (已修改)
-- 4. Order-Status Transaction (需修改;Done)
-- 5. Stock-Level Transaction (需修改 - 用v1;Done)
-- 6. Popular-Item Transaction (准备做 - 用v1;Done)
-- 7. Top-Balance Transaction (已修改 - 用v1;Done)

-- 1. New Order Transaction 

-- step1,2
update District set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = 'W_ID' and D_ID = 'D_ID' returning D_NEXT_O_ID;
-- 得到 'N' = D_NEXT_O_ID - 1

-- 创建订单信息临时表 new_order_info
create table new_order_info
    (NO_O_ID int NOT NULL, 
    NO_N int NOT NULL, 
    NO_W_ID int NOT NULL,  
    NO_D_ID int NOT NULL, 
    NO_C_ID int NOT NULL, 
    NO_I_CNT int NOT NULL, 
    NO_I_ID int NOT NULL, 
    NO_SUPPLY_W_ID int NOT NULL, 
    NO_QUANTITY decimal(2,0) NOT NULL, 
    primary key (NO_O_ID, NO_N, NO_W_ID, NO_D_ID, NO_C_ID));

-- 设 NO_ALL_LOCAL = 1
-- 读取txt中item数据 for (int i = 1; i <= 'M'; i++)
    insert into new_order_info
    values ('N', i, 'W_ID', 'D_ID', 'C_ID', 'M', 'OL_I_ID', 'OL_SUPPLY_W_ID', 'OL_QUANTITY');
    -- if 'W_ID' != 'OL_SUPPLY_W_ID', NO_ALL_LOCAL = 0

-- step3
-- 取出当前时间 current_time = current_timestamp
insert into Orders (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
values ('N', 'D_ID', 'W_ID', 'C_ID', 'current_time', NULL, 'M', 'NO_ALL_LOCAL'); 

-- step4
TOTAL_AMOUNT = 0

--step5 - Stock
update Stock
set S_QUANTITY = ADJUSTED_QTY, S_YTD = NO_YTD, S_ORDER_CNT = NO_ORDER_CNT, S_REMOTE_CNT = NO_REMOTE_CNT
from (select
        t1.NO_SUPPLY_W_ID,
        t1.NO_I_ID,
        case when t1.NO_QUANTITY - t2.S_QUANTITY < 10 then t1.NO_QUANTITY - t2.S_QUANTITY + 100 else t1.NO_QUANTITY - t2.S_QUANTITY end as ADJUSTED_QTY,
        t1.NO_QUANTITY as NO_YTD,
        t2.S_ORDER_CNT + 1 as NO_ORDER_CNT,
        S_REMOTE_CNT + case when t1.NO_SUPPLY_W_ID != t1.NO_W_ID then 1 else 0 end as NO_REMOTE_CNT
    from new_order_info t1 left join Stock t2
    on t1.NO_SUPPLY_W_ID = t2.S_W_ID and t1.NO_I_ID = t2.S_I_ID) t
where S_W_ID = t.NO_SUPPLY_W_ID and S_I_ID = t.NO_I_ID;

-- step5 - OrderLine
insert into OrderLine
select NO_W_ID, NO_D_ID, NO_O_ID, NO_N, NO_I_ID, NULL, NO_QUANTITY * I_PRICE as ITEM_AMOUNT, NO_SUPPLY_W_ID, NO_QUANTITY, CONCAT('S_DIST_', D_ID) as NO_DIST_INFO
from new_order_info t1
left join District t2 on t1.NO_W_ID = t2.D_W_ID and t1.NO_D_ID = t2.D_ID
left join Item t3 on t1.NO_I_ID = t3.I_ID;

-- step6
select sum(OL_AMOUNT) as TOTAL_AMOUNT from OrderLine where OL_O_ID = 'N' and OL_D_ID = 'D_ID' and OL_W_ID = 'W_ID';
-- 得到 TOTAL_AMOUNT 中间值

select W_TAX, D_TAX, C_LAST, C_CREDIT, C_DISCOUNT from Warehouse t1 
left join District t2 on t1.W_ID = t2.D_W_ID 
left join Customer t3 on t2.D_W_ID = t3.C_W_ID and t2.D_ID = t3.C_D_ID
where D_W_ID = 'W_ID' and D_ID = 'D_ID' and C_ID = 'C_ID';
-- 得到 W_TAX, D_TAX, C_LAST, C_CREDIT, C_DISCOUNT 

-- 计算 TOTAL_AMOUNT = TOTAL_AMOUNT * (1+ D_TAX +W_TAX) * (1 - C_DISCOUNT)

-- 输出结果 'W_ID', 'D_ID', 'C_ID', C_LAST, C_CREDIT, C_DISCOUNT, W_TAX, D_TAX, 'N', 'current_time', 'M', TOTAL_AMOUNT

select NO_I_ID, I_NAME, NO_SUPPLY_W_ID, NO_QUANTITY, NO_QUANTITY * I_PRICE as OL_AMOUNT, S_QUANTITY
from new_order_info t1
left join Item t2 on t1.NO_I_ID = t2.I_ID 
left join Stock t3 on t1.NO_I_ID = t3.S_I_ID and t1.NO_SUPPLY_W_ID = t3.S_W_ID;
--输出 NO_I_ID, I_NAME, NO_SUPPLY_W_ID, NO_QUANTITY, OL_AMOUNT, S_QUANTITY

-- 保存结果到customer_item
insert into customer_item select NO_W_ID, NO_D_ID, NO_C_ID, NO_O_ID, NO_I_ID, NO_N from new_order_info;

-- 删除new order表
drop table new_order_info

-- 4. Order-Status Transaction
select
    C_FIRST,
    C_MIDDLE,
    C_LAST,
    C_BALANCE
from
    Customer
where 
    C_W_ID = 'C_W_ID'
    and C_D_ID = 'C_D_ID'
    and C_ID = 'C_ID'
;
-- 修改部分%%
select 
    O_ID,
    O_ENTRY_D,
    O_CARRIER_ID
from 
    Orders
where
    O_W_ID = 'C_W_ID'
    and O_D_ID = 'C_D_ID'
    and O_C_ID = 'C_ID'
order by
    O_ID desc
limit 1
--%%
-- 拿到'O_ID'

select 
    OL_I_ID,
    OL_SUPPLY_W_ID,
    OL_QUANTITY,
    OL_AMOUNT,
    OL_DELIVERY_D
from
    OrderLine
where
    OL_W_ID = 'C_W_ID'
    OL_D_ID = 'C_D_ID'
    OL_O_ID = 'O_ID'
;

-- 5. Stock-Level Transaction

-- version 1 (无中间值)
--修改部分%%
select 
    D_NEXT_O_ID
from 
    District
where
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'

-- 得到 N = D_NEXT_O_ID

with last_l_ol_orders as(
    select 
        *
    from 
        OrderLine
    where 
        OL_W_ID = 'W_ID'
        and OL_D_ID = 'D_ID'
        and OL_O_ID >= 'N'-'L'
        and OL_O_ID < 'N'
)
--%%
select 
    count(distinct S_I_ID) as item_cnt
from
    last_l_ol_orders t1
left join 
    Stock t2
on 
    t1.OL_W_ID = t2.S_W_ID
    and t1.OL_I_ID = t2.S_I_ID
where
    S_QUANTITY < 'T'

-- version 2 (有中间值)
select 
    *
from 
    OrderLine
where 
    OL_W_ID = 'W_ID'
    and OL_D_ID = 'D_ID'
order by 
    OL_O_ID desc
limit 'L'

-- 得到最新L个订单中的item(OL_I_ID)集合IL

select 
    count(S_I_ID) as item_cnt
from
    Stock
where
    S_W_ID = 'W_ID'
    and S_I_ID in 'IL'
    and S_QUANTITY < 'T'

-- 6. Popular-Item Transaction

-- new version (无中间值)
---- SQL1 start
select 
    D_NEXT_O_ID
from 
    District
where
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'

-- 得到 N
---- SQL1 end

---- SQL2 start
with last_l_orders as (
    select 
        *
    from 
        Orders
    where
        O_W_ID = 'W_ID'
        and O_D_ID = 'D_ID'
        and O_ID >= 'N'-'L'
        and O_ID < 'N'
)

select 
    t1.O_ID,
    t1.O_ENTRY_D,
    t2.C_FIRST,
    t2.C_MIDDLE,
    t2.C_LAST
from 
    last_l_orders t1
left join
    Customer t2
on
    t1.O_W_ID = t2.C_W_ID
    and t1.O_D_ID = t2.C_D_ID
    and t1.O_ID = t2.C_ID
;
---- SQL2 end

---- SQL3 start
with last_l_orders as (
    select 
        *
    from 
        Orders
    where
        O_W_ID = 'W_ID'
        and O_D_ID = 'D_ID'
        and O_ID >= 'N'-'L'
        and O_ID < 'N'
),

last_l_orders_items as (
    select
        *,
        rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank
    from
        last_l_orders t1
    left join
        OrderLine t2
    on
        t1.O_W_ID = t2.OL_W_ID
        and t1.O_D_ID = t2.OL_D_ID
        and t1.O_ID = t2.OL_O_ID
)

select
    t1.O_ID,
    t2.I_NAME,
    t1.OL_QUANTITY
from
    last_l_orders_items t1
left join 
    Item t2
on 
    t1.OL_I_ID = t2.I_ID
where 
    t1.rank = 1
order by 
    t1.O_ID
;
---- SQL3 end


---- SQL4 start
with last_l_orders as (
    select 
        *
    from 
        Orders
    where
        O_W_ID = 'W_ID'
        and O_D_ID = 'D_ID'
        and O_ID >= 'N'-'L'
        and O_ID < 'N'
),

last_l_orders_items as (
    select
        *,
        rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank
    from
        last_l_orders t1
    left join
        OrderLine t2
    on
        t1.O_W_ID = t2.OL_W_ID
        and t1.O_D_ID = t2.OL_D_ID
        and t1.O_ID = t2.OL_O_ID
)

select
    t3.I_NAME,
    count(t2.OL_I_ID) * 100 / 'L'  as I_Percentage
from
    (select distinct OL_I_ID from last_l_orders_items where rank = 1) t1
left join
    last_l_orders_items t2 
on 
    t1.OL_I_ID = t2.OL_I_ID
left join 
    Item t3
on 
    t1.OL_I_ID = t3.I_ID
group by 
    t3.I_NAME
;
---- SQL4 end

-- version 2 (有中间值)

select 
    *
from 
    (select 
        *,
        row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank
    from 
        Orders
    where
        O_W_ID = 'W_ID'
        and O_D_ID = 'D_ID'
    ) t
where rank <= 'L'

-- 得到最后L个订单集合OL(O_W_ID, O_D_ID, O_ID, O_C_ID, O_ENTRY_D)

select * from (
    select
        *,
        rank()over(partition by O_W_ID, O_D_ID, O_ID order by OL_QUANTITY desc) as rank
    from 
        last_l_orders t1
    left join 
        OrderLine t2
    on 
        t1.O_W_ID = t2.OL_W_ID
        and t1.O_D_ID = t2.OL_D_ID
        and t1.O_ID = t2.OL_O_ID
) t
where 
    rank = 1

-- 得到popular items集合IL(OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID)

select 
    C_FIRST,
    C_MIDDLE,
    C_LAST
from 
    Customer
where 
    C_W_ID, C_D_ID, C_ID in 'OL(O_W_ID, O_D_ID, O_ID)'
;

select 
    I_NAME
from
    Item
where
    I_ID in 'IL(OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID)'
;

select 
    t1.I_NAME,
    count(t2.OL_I_ID) / 'L' * 100 as I_Percentage
from (
    select distinct 
        I_ID, 
        I_NAME
    from
        Item
    where
        I_ID in 'IL(OL_I_ID)'
) t1
left join (
    select 
        *
    from
        OrderLine
    where
        OL_O_ID in 'OL(O_W_ID, O_D_ID, O_ID)'
) t1
    OrderLine t2
on 
    t1.I_ID = t2.OL_I_ID
group by 
    t1.I_NAME

-- 7. Top-Balance Transaction

-- version 1 (无中间值)
with top_10_customers as(
    select
    *
    from
        Customer  
    order by
        C_BALANCE desc
    limit 10
)
select
    t1.C_FIRST,
    t1.C_MIDDLE,
    t1.C_LAST,
    t1.C_BALANCE,
    t2.W_NAME,
    t3.D_NAME
from 
    top_10_customers t1 
left join 
    Warehouse t2
on 
    t1.C_W_ID = t2.W_ID
left join 
    District t3
on 
    t1.C_D_ID = t3.D_ID

-- version 2 (有中间值)

select 
    C_FIRST,
    C_MIDDLE,
    C_LAST,
    C_BALANCE,
    C_W_ID, 
    C_D_ID
from 
    Customer  
order by
    C_BALANCE desc
limit 10

-- 得到TOP10 customer的 W,D集合 W10, D10

select 
    W_NAME
from 
    Warehouse
where
    W_ID in W10
;

select 
    D_NAME
from 
    District
where
    D_ID in D10