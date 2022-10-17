-- YCQL
-- 1. New Order Transaction
-- 4. Order-Status Transaction (checked)
-- 5. Stock-Level Transaction (checked)
-- 6. Popular-Item Transaction (finished)
-- 7. Top-Balance Transaction (finished)

-- 1. New Order Transaction


-- 4. Order-Status Transaction
-- e.g. O,1,1,1771

--CQL1
select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from Customer
where C_W_ID = 'C_W_ID' and C_D_ID = 'C_D_ID' and C_ID = 'C_ID';

--CQL2
select O_ID, O_ENTRY_D, O_CARRIER_ID from Orders
where O_W_ID = 'C_W_ID' and O_D_ID = 'C_D_ID' and O_C_ID = 'C_ID'
order by O_ID desc limit 1 allow filtering;
-- 拿到'O_ID'

--CQL3
select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from OrderLine
where OL_W_ID = 'C_W_ID' and OL_D_ID = 'C_D_ID' and OL_O_ID = 'O_ID';

-- 5. Stock-Level Transaction
-- e.g. S,1,1,14,27

--CQL1
select D_NEXT_O_ID from District 
where D_W_ID = 'W_ID' and D_ID = 'D_ID';
-- 得到 N = D_NEXT_O_ID

-- CQL2
select OL_I_ID  from OrderLine
where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID >= 'N'-'L' and OL_O_ID < 'N'
allow filtering;
-- 得到'OL_I_ID'的集合IL (需要对'OL_I_ID'去重)

-- CQL3
-- 设num = 0; 
-- for OL_I_ID in IL:
    select S_QUANTITY from Stock
    where S_W_ID = 'W_ID' and S_I_ID = 'OL_I_ID'
    allow filtering;
    -- if S_QUANTITY < 'T', num += 1
-- 循环结束后输出num

-- 6. Popular-Item Transaction

--CQL1
select D_NEXT_O_ID from District 
where D_W_ID = 'W_ID' and D_ID = 'D_ID';
-- 得到 N = D_NEXT_O_ID

--CQL2
select O_C_ID, O_ID, O_ENTRY_D from Orders
where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N'
allow filtering;
-- 得到最新L个订单信息 (O_C_ID, O_ID, O_ENTRY_D)

-- CQL3
--for every O_C_ID:
    select C_FIRST, C_MIDDLE, C_LAST from Customer
    where C_W_ID = 'W_ID' and C_D_ID = 'D_ID' and C_ID = 'OL_C_ID';
    -- 得到每个订单的用户信息 (C_FIRST, C_MIDDLE, C_LAST)

--for every O_ID:
    -- CQL4
    select OL_W_ID, OL_D_ID, OL_O_ID, OL_QUANTITY as MAX_OL_QUANTITY from OrderLine
    where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID = 'O_ID'
    order by OL_QUANTITY desc limit 1 allow filtering;
    -- 得到 MAX_OL_QUANTITY = OL_QUANTITY
    -- CQL5
    select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID
    where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID = 'OL_O_ID' and OL_QUANTITY = 'MAX_OL_QUANTITY'
    allow filtering;
    -- 得到当前订单的所有 OL_I_ID (并用另一个集合记录下所有订单的 OL_I_ID)
    -- for every OL_I_ID:
        -- CQL6
        select I_NAME from Item where I_ID = 'OL_I_ID';
        -- 输出 I_NAME, MAX_OL_QUANTITY

-- for every OL_I_ID:
    -- CQL7
    select I_NAME from Item where I_ID = 'OL_I_ID';
    -- CQL8
    select OL_I_ID, count(*) * 100 /'L' as I_Percentage from OrderLine
    where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID >= 'N'-'L' and OL_O_ID < 'N' and OL_I_ID = 'OL_I_ID'
    group by OL_I_ID
    allow filtering;
    -- 输出 I_NAME, I_Percentage

-- 7. Top-Balance Transaction

--CQL1
select C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_ID, C_D_ID from Customer  
order by C_BALANCE desc limit 10;
--得到top 10 customer的全部信息

-- for every C_ID:
    --CQL2
    select W_NAME from Warehouse where W_ID = 'C_W_ID';
    --CQL3
    select D_NAME from District where D_ID = 'C_D_ID';
    -- 输出当前customer: C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_ID, C_D_ID, W_NAME, D_NAME