-- YSQL
-- 1. New Order Transaction (准备做)
-- 4. Order-Status Transaction (需修改;Done)
-- 5. Stock-Level Transaction (需修改 - 用v1;Done)
-- 6. Popular-Item Transaction (准备做 - 用v1;Done)
-- 7. Top-Balance Transaction (已修改 - 用v1;Done)

-- 1. New Order Transaction 

-- step1,2
update 
    District
set 
    D_NEXT_O_ID = D_NEXT_O_ID + 1
where 
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'
-- return(returning) D_NEXT_O_ID

select
    D_NEXT_O_ID
from 
    District
where 
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'

'N' = D_NEXT_O_ID --(最新的)

-- 创建订单信息临时表 new_order_info
create table 
    new_order_info
    (NO_O_ID, NO_N, NO_W_ID, NO_D_ID, NO_C_ID, NO_I_ID, NO_SUPPLY_W_ID, NO_QUANTITY primary key (NO_O_ID, NO_N))

-- for (int i = 1; i <= 'NUM_ITEMS'; i++)
insert into 
    new_order_info
values 
    'N', i, 'W_ID', 'D_ID', 'C_ID', 'OL_I_ID', 'OL_SUPPLY_W_ID', 'OL_QUANTITY'

-- step3
insert into 
    Orders (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
values
    'N', 'D_ID', 'W_ID', 'C_ID', current_timestamp, NULL, 'NUM_ITEMS',
    if('W_ID' != any(select NO_SUPPLY_W_ID from new_order_info), 0, 1)

-- step4
TOTAL_AMOUNT = 0

--step5 - Stock
--for (int i = 1; i <= 'NUM_ITEMS'; i++)
update 
    Stock
set 
    S_QUANTITY = ADJUSTED_QTY,
    S_YTD = NO_YTD
    S_ORDER_CNT = NO_ORDER_CNT
    S_REMOTE_CNT = NO_REMOTE_CNT
from (
    select
        t2.S_W_ID,
        t2.S_I_ID,
        if(t1.NO_QUANTITY - t2.S_QUANTITY < 10, t1.NO_QUANTITY - t2.S_QUANTITY + 100, t1.NO_QUANTITY - t2.S_QUANTITY) as ADJUSTED_QTY,
        t1.NO_QUANTITY as NO_YTD,
        t2.S_ORDER_CNT + 1 as NO_ORDER_CNT,
        S_REMOTE_CNT + if(t1.NO_SUPPLY_W_ID != t1.W_ID, 1, 0) as NO_REMOTE_CNT
    from 
        new_order_info t1
    left join 
        Stock t2
    on 
        t1.NO_W_ID = t2.S_W_ID
        and t1.NO_I_ID = t2.S_I_ID
) t
where 
    S_W_ID = 'W_ID'
    and S_I_ID = 'OL_I_ID'

-- step5 - OrderLine
insert into 
    OrderLine
    (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D, OL_DIST_INFO)
select
    NO_O_ID, NO_D_ID, NO_W_ID, NO_N, NO_I_ID, NO_SUPPLY_W_ID, NO_QUANTITY, 
    NO_QUANTITY * I_PRICE as ITEM_AMOUNT, 
    NULL::TIMESTAMP as NO_DELIVERY_D,
    CONCAT(\'S_DIST_\',D_ID::STRING) as NO_DIST_INFO
from
    new_order_info t1
left join
    District t2
on 
    t1.NO_W_ID = t2.D_W_ID
    and t1.NO_D_ID = t2.D_ID
left join 
    Item t3
on 
    t1.NO_I_ID = t3.I_ID

-- 结束循环
select 
    sum(OL_AMOUNT) as TOTAL_AMOUNT
from 
    OrderLine
where
    OL_O_ID = 'N',
    OL_D_ID = 'D_ID', 
    OL_W_ID = 'W_ID'

-- step6
select 
    W_TAX, 
    D_TAX,
    C_LAST, 
    C_CREDIT, 
    C_DISCOUNT
from 
    Warehouse t1
left join 
    District t2
on 
    t1.W_ID = t2.D_W_ID
left join 
    Customer t3
on  
    t2.D_W_ID = t3.C_W_ID
    and t2.D_ID = t3.C_D_ID
where
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'

TOTAL_AMOUNT = TOTAL_AMOUNT * (1+ D_TAX +W_TAX) * (1 - C_DISCOUNT)

-- get result



drop table 
    new_order_info

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

-- 得到 N =  D_NEXT_O_ID + 1

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

-- version 1 (无中间值)
---- SQL1 start
with last_l_orders as (
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
    t2.O_ENTRY_D,
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
---- SQL1 end

---- SQL2 start
with last_l_orders as (
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
---- SQL2 end


---- SQL3 start
with last_l_orders as (
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
    count(t2.OL_I_ID) / 'L' * 100 as I_Percentage
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
    t2.I_NAME
;
---- SQL3 end

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
        C_FIRST,
        C_MIDDLE,
        C_LAST,
        C_BALANCE,
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