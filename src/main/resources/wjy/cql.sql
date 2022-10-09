-- YCQL
-- 1. New Order Transaction
-- 4. Order-Status Transaction (finished)
-- 5. Stock-Level Transaction (finished)
-- 6. Popular-Item Transaction (finished)
-- 7. Top-Balance Transaction (finished)

-- 1. New Order Transaction


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
allow filtering
order by
    O_ID desc
limit 1
;

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
select 
    D_NEXT_O_ID
from 
    District
where
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'

-- 得到 N =  D_NEXT_O_ID + 1

select 
    distinct OL_I_ID
from 
    OrderLine
where 
    OL_W_ID = 'W_ID'
    and OL_D_ID = 'D_ID'
    and OL_O_ID >= 'N'-'L'
    and OL_O_ID < 'N'
allow filtering

-- 得到最新L个订单中的item(OL_I_ID)集合IL
-- num=0; for loop (OL_I_ID in IL) 
select 
    S_I_ID
from
    Stock
where
    S_W_ID = 'W_ID'
    and S_I_ID = 'OL_I_ID' --IL for loop
    and S_QUANTITY < 'T'
allow filtering
-- if not null: num += 1

-- 6. Popular-Item Transaction

select 
    D_NEXT_O_ID
from 
    District
where
    D_W_ID = 'W_ID'
    and D_ID = 'D_ID'

-- 得到 N =  D_NEXT_O_ID + 1

select
    O_C_ID,
    O_ID,
    O_ENREY_D
from 
    Orders
where
    O_W_ID = 'W_ID'
    and O_D_ID = 'D_ID'
    and O_ID >= 'N'-'L'
    and O_ID < 'N'
allow filtering

-- 得到最新L个订单的集合OL1(O_ID, O_C_ID)
-- 输出O_ID, O_ENREY_D
--for loop OL_C_ID in (OL1)
select 
    C_FIRST,
    C_MIDDLE,
    C_LAST
from 
    Customer
where 
    C_W_ID = 'W_ID'
    C_D_ID = 'D_ID'
    C_ID = 'OL_C_ID' --OL1 for loop
;
--for loop OL1_O_ID in (OL1)
select
    OL_W_ID,
    OL_D_ID,
    OL_O_ID,
    max(OL_QUANTITY) as MAX_OL_QUANTITY
from 
    OrderLine
where 
    OL_W_ID = 'W_ID'
    and OL_D_ID = 'D_ID'
    and OL_O_ID = 'OL1_O_ID' --OL1 for loop
group by 
    OL_W_ID,
    OL_D_ID,
    OL_O_ID

-- 得到最新L个订单的OL2(OL_O_ID, MAX_OL_QUANTITY)
--OL2 for loop (OL2_O_ID)
select 
    OL_W_ID,
    OL_D_ID,
    OL_O_ID,
    OL_I_ID
where 
    OL_W_ID = 'W_ID'
    and OL_D_ID = 'D_ID'
    and OL_O_ID = 'OL_O_ID' --OL for loop
    and OL_QUANTITY = 'MAX_OL_QUANTITY'
allow filtering
--输出结果
--得到最新L个订单的popular item集合ILP(set)
-- for loop (OL_I_ID in ILP)
select 
    I_NAME
from
    Item
where
    I_ID = 'OL_I_ID' -- ILP for loop
;

select 
    OL_I_ID, 
    count(*)/'L' * 100 as I_Percentage
from 
    order_line_rich 
where 
    OL_W_ID = 'W_ID'
    and OL_D_ID = 'D_ID'
    and OL_O_ID >= 'N'
    and OL_O_ID < 'N'-'L'
    and OL_I_ID = 'OL_I_ID' -- ILP for loop
allow filtering

-- 7. Top-Balance Transaction
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

-- 输出结果
-- 得到TOP10 customer的 W,D 集合set W10, D10
-- for loop (C_W_ID in W10)
select 
    W_NAME
from 
    Warehouse
where
    W_ID = 'C_W_ID' --(for loop)
;
-- for loop (C_D_ID in D10)
select 
    D_NAME
from 
    District
where
    D_ID = 'C_D_ID' --(for loop)