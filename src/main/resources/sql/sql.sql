-- YSQL

-- 1. New Order Transaction 
update 
    District
set 
    D_NEXT_O_ID += 1
where 
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'

insert into 
    Order (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
values
    ('N', 'D_ID', 'W_ID', 'C_ID', current_timestamp, NULL, 'NUM_ITEMS',
    case when )
    
update 
    Stock
set 
    S_QUANTITY = 
    S_YTD = 
    S_ORDER_CNT += 1,
    S_REMOTE_CNT = 
where 
    --D_W_ID = 'W_ID'
    --and D_ID = 'D_ID'

insert into 
    OrderLine 
values
    ( )




-- 2. Payment Transaction

-- 3. Delivery Transaction

-- 4. Order-Status Transaction (已确认)
select distinct
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

select 
    O_ID,
    O_ENTRY_D,
    O_CARRIER_ID
from 
    (select 
        *,
        row_number()over(partition by O_W_ID, O_D_ID, O_C_ID order by O_ENTRY_D desc) as rank
    from 
        Order
    where
        O_W_ID = 'C_W_ID'
        and O_D_ID = 'C_D_ID'
        and O_C_ID = 'C_ID'
    ) t
where rank = 1

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

-- 5. Stock-Level Transaction (已确认)
with last_l_ol_orders as(
    select 
        *
    from (
        select
            *,
            rank()over(partition by OL_W_ID, C_D_ID order by OL_O_ID desc) as rank
        from 
            OrderLine
        where 
            OL_W_ID = 'W_ID'
            and C_D_ID = 'D_ID'
    ) t 
    where rank < = 'L'
)

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

-- 6. Popular-Item Transaction
with last_l_orders as (
    select 
        *
    from 
        (select 
            *,
            row_number()over(partition by O_W_ID, O_D_ID order by O_ID desc) as rank
        from 
            Order
        where
            O_W_ID = 'W_ID'
            and O_D_ID = 'D_ID'
        ) t
    where rank <= 'L'
),

popular_items as (
    select 
        *
    from(
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



select 
    t2.I_NAME,
    t1.OL_QUANTITY
from
    popular_items t1
left join 
    Item t2
on 
    t1.OL_I_ID = t2.I_ID

-- 7. Top-Balance Transaction (已确认)
with top_10_customers as(
    select 
        C_FIRST,
        t1.C_MIDDLE,
        C_LAST,
        C_BALANCE,
    from(
        select
            *,
            row_number()over(order by C_BALANCE desc) as rank
        from 
            Customer    
    )
    where rank <= 10
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

-- 8. Related-Customer Transaction

-- YCQL

-- 1. New Order Transaction 
    
-- 2. Payment Transaction

-- 3. Delivery Transaction

-- 4. Order-Status Transaction

-- 5. Stock-Level Transaction

-- 6. Popular-Item Transaction

-- 7. Top-Balance Transaction

-- 8. Related-Customer Transaction


-- 6. Popular-Item Transaction 
-- 中间值