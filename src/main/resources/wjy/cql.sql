-- YCQL
-- 1. New Order Transaction (finished)
-- 4. Order-Status Transaction (finished)
-- 5. Stock-Level Transaction (finished)
-- 6. Popular-Item Transaction (finished)
-- 7. Top-Balance Transaction (finished)

-- 1. New Order Transaction
--CQL1
-- 读取第一行 N 后面的数据
select D_NEXT_O_ID from dbycql.District where D_W_ID = 'W_ID' and D_ID = 'D_ID';
--CQL2
-- 得到 'N' = D_NEXT_O_ID
update dbycql.District set D_NEXT_O_ID = D_NEXT_O_ID + 1 where D_W_ID = 'W_ID' and D_ID = 'D_ID';

-- 设 TOTAL_AMOUNT = 0, NO_ALL_LOCAL = 1

-- for (int i = 1; i <= 'M'; i++)
    -- 读取输入的每行item的数据
    -- if 'W_ID' != 'OL_SUPPLY_W_ID', then NO_ALL_LOCAL = 0
    -- if 'W_ID' != 'OL_SUPPLY_W_ID', then IF_REMOTE = 1, else IF_REMOTE = 0

    --CQL3
    --update Stock
    select S_QUANTITY from dbycql.Stock where S_W_ID = 'OL_SUPPLY_W_ID' and S_I_ID = 'OL_I_ID';
    --CQL4
    -- ADJUSTED_QTY = S_QUANTITY - 'OL_QUANTITY', 进行判断：if ADJUSTED_QTY < 10, then ADJUSTED_QTY += 100
    select S_YTD from dbycql.Stock where S_W_ID = 'OL_SUPPLY_W_ID' and S_I_ID = 'OL_I_ID';
    -- S_YTD_NEW = S_YTD + 'OL_QUANTITY'
    -- CQL5
    update dbycql.Stock
    set S_QUANTITY = 'ADJUSTED_QTY', S_YTD = 'S_YTD_NEW', S_ORDER_CNT = S_ORDER_CNT + 1, S_REMOTE_CNT = S_REMOTE_CNT + 'IF_REMOTE'
    where S_W_ID = 'OL_SUPPLY_W_ID' and S_I_ID = 'OL_I_ID';

    -- update OrderLine
    -- CQL6
    select I_NAME, I_PRICE from dbycql.Item where I_ID = 'OL_I_ID';
    -- ITEM_AMOUNT = I_PRICE * 'OL_QUANTITY'
    -- TOTAL_AMOUNT += ITEM_AMOUNT
    -- DIST_INFO = 'S_DIST_' + 'D_ID' (string 连接)
    -- CQL7
    insert into dbycql.OrderLine (OL_W_ID, OL_D_ID, OL_O_ID, OL_NUMBER, OL_I_ID, OL_DELIVERY_D, OL_AMOUNT, OL_SUPPLY_W_ID, OL_QUANTITY, OL_DIST_INFO)
    values ('W_ID', 'D_ID', 'N', i, 'OL_I_ID', NULL, 'ITEM_AMOUNT', 'OL_SUPPLY_W_ID', 'OL_QUANTITY', 'DIST_INFO');

    -- update dbycql.customer_item
    -- CQL8
    insert into dbycql.customer_item (CI_W_ID, CI_D_ID, CI_C_ID, CI_O_ID, CI_I_ID, CI_I_NUMBER)
    values ('W_ID', 'D_ID', 'C_ID', 'N', 'OL_I_ID', i);

    -- 输出结果 i, I_NAME, 'OL_SUPPLY_W_ID', 'OL_QUANTITY', ITEM_AMOUNT, ADJUSTED_QTY

-- 结束循环

-- 取出当前时间 current_time = current_timestamp
-- CQL9
insert into dbycql.Orders (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_CARRIER_ID, O_OL_CNT, O_ALL_LOCAL)
values ('N', 'D_ID', 'W_ID', 'C_ID', toTimestamp(now()), NULL, 'M', 'NO_ALL_LOCAL');

-- step6
-- CQL10
select W_TAX from dbycql.Warehouse where W_ID = 'W_ID';
-- CQL11
select D_TAX from dbycql.District where D_W_ID = 'W_ID' and D_ID = 'D_ID';
-- CQL12
select C_LAST, C_CREDIT, C_DISCOUNT from dbycql.Customer where C_W_ID = 'W_ID' and C_D_ID = 'D_ID' and C_ID = 'C_ID';
-- 计算 TOTAL_AMOUNT = TOTAL_AMOUNT * (1+ D_TAX +W_TAX) * (1 - C_DISCOUNT)

-- 最后输出结果 'W_ID', 'D_ID', 'C_ID', C_LAST, C_CREDIT, C_DISCOUNT, W_TAX, D_TAX, 'N', 'current_time', 'M', TOTAL_AMOUNT


-- 4. Order-Status Transaction

--CQL1
select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from dbycql.Customer
where C_W_ID = 'C_W_ID' and C_D_ID = 'C_D_ID' and C_ID = 'C_ID';
--copy
select C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from dbycql.Customer
where C_W_ID = %d and C_D_ID = %d and C_ID = %d

--CQL2
select O_ID, O_ENTRY_D, O_CARRIER_ID from dbycql.Orders
where O_W_ID = 'C_W_ID' and O_D_ID = 'C_D_ID' and O_C_ID = 'C_ID'
order by O_ID desc limit 1;
--copy
select O_ID, O_ENTRY_D, O_CARRIER_ID from dbycql.Orders
where O_W_ID = %d and O_D_ID = %d and O_C_ID = %d
order by O_ID desc limit 1;
-- 拿到'O_ID'

--CQL3
select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from dbycql.OrderLine
where OL_W_ID = 'C_W_ID' and OL_D_ID = 'C_D_ID' and OL_O_ID = 'O_ID';
--copy
select OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DELIVERY_D from dbycql.OrderLine
where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID = %d

-- 5. Stock-Level Transaction

--CQL1
select D_NEXT_O_ID from dbycql.District where D_W_ID = 'W_ID' and D_ID = 'D_ID';
--copy
select D_NEXT_O_ID from dbycql.District where D_W_ID = %d and D_ID = %d
-- 得到 N = D_NEXT_O_ID

-- CQL2
select OL_I_ID from dbycql.OrderLine
where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID >= 'N'-'L' and OL_O_ID < 'N'
allow filtering;
--copy
select OL_I_ID from dbycql.OrderLine
where OL_W_ID = %d and OL_D_ID = %d and OL_O_ID >= %d - %d and OL_O_ID < %d
allow filtering
-- 得到'OL_I_ID'的集合IL (需要对'OL_I_ID'去重)

-- CQL3
-- 设num = 0; 
-- for OL_I_ID in IL:
    select S_QUANTITY from dbycql.Stock
    where S_W_ID = 'W_ID' and S_I_ID = 'OL_I_ID'
    allow filtering;
    -- if S_QUANTITY < 'T', num += 1
-- 循环结束后输出num

-- 6. Popular-Item Transaction

--CQL1
select D_NEXT_O_ID from dbycql.District where D_W_ID = 'W_ID' and D_ID = 'D_ID';
-- 得到 N = D_NEXT_O_ID

--CQL2
select O_C_ID, O_ID, O_ENTRY_D from dbycql.Orders
where O_W_ID = 'W_ID' and O_D_ID = 'D_ID' and O_ID >= 'N'-'L' and O_ID < 'N';
-- 得到最新L个订单的信息 (O_C_ID, O_ID, O_ENTRY_D)

--for every O_ID:
    -- CQL3
    select C_FIRST, C_MIDDLE, C_LAST from dbycql.Customer
    where C_W_ID = 'W_ID' and C_D_ID = 'D_ID' and C_ID = 'O_C_ID';
    -- 得到每个订单的用户信息 (C_FIRST, C_MIDDLE, C_LAST)
    -- CQL4
    select OL_W_ID, OL_D_ID, OL_O_ID, OL_QUANTITY from dbycql.OrderLine
    where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID = 'O_ID' limit 1;
    -- 得到 MAX_OL_QUANTITY = OL_QUANTITY
    -- CQL5
    select OL_W_ID, OL_D_ID, OL_O_ID, OL_I_ID from dbycql.OrderLine
    where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID = 'OL_O_ID' and OL_QUANTITY = 'MAX_OL_QUANTITY';

    -- 得到当前订单的所有 OL_I_ID (并用另一个集合(all_item_set)记录下所有订单的 OL_I_ID(去重))
    -- for every OL_I_ID:
        -- CQL6
        select I_NAME from dbycql.Item where I_ID = 'OL_I_ID';
        -- 输出 I_NAME, MAX_OL_QUANTITY

-- for every OL_I_ID in all_item_set:
    -- CQL7
    select I_NAME from dbycql.Item where I_ID = 'OL_I_ID';
    -- CQL8
    select count(OL_I_ID) as I_NUM from dbycql.OrderLine
    where OL_W_ID = 'W_ID' and OL_D_ID = 'D_ID' and OL_O_ID >= 'N'-'L' and OL_O_ID < 'N' and OL_I_ID = 'OL_I_ID';
    -- 输出 I_NAME, I_Percentage = I_NUM * 100 / 'L'

-- 7. Top-Balance Transaction

-- 新建top10 customer table
-- CQL1
CREATE TABLE dbycql.customer_balance_top10 (
    cb_top10 text,
    cb_w_id int,
    cb_d_id int,
    cb_id int,
    cb_first text,
    cb_middle text,
    cb_last text,
    cb_balance decimal,
    cb_time timeuuid,
    PRIMARY KEY ((cb_top10), cb_balance, cb_time)
) WITH CLUSTERING ORDER BY (cb_balance DESC, cb_time);

-- for every C_W_ID (1-10):
    -- for every C_D_ID (1-10):
        -- CQL2
        select C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_BALANCE from dbycql.customer
        where C_W_ID = 'C_W_ID' and C_D_ID = 'C_D_ID' limit 10;
        -- 得到 'C_W_ID', 'C_D_ID', 'C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_BALANCE'
        -- 一条一条插入数据 for every record(10条):
            -- CQL3
            insert into dbycql.customer_balance_top10 (CB_TOP10, CB_W_ID, CB_D_ID, CB_ID, CB_FIRST, CB_MIDDLE, CB_LAST, CB_BALANCE, CB_TIME) 
            values ('top10', 'C_W_ID', 'C_D_ID', 'C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_BALANCE' now());

--get all top10 customer
-- CQL4
select CB_W_ID, CB_D_ID, CB_ID, CB_FIRST, CB_MIDDLE, CB_LAST, CB_BALANCE from dbycql.customer_balance_top10 limit 10;
-- 得到 'C_W_ID', 'C_D_ID', 'C_ID', 'C_FIRST', 'C_MIDDLE', 'C_LAST', 'C_BALANCE'

-- for every top10 customer:
    -- CQL5
    select W_NAME from dbycql.Warehouse where W_ID = 'C_W_ID';
    -- CQL6
    select D_NAME from dbycql.District where D_W_ID = 'C_W_ID' and D_ID = 'C_D_ID';
    -- 输出当前customer: C_FIRST, C_MIDDLE, C_LAST, C_BALANCE, C_W_ID, C_D_ID, W_NAME, D_NAME

-- 删除临时表
-- CQL7
DROP TABLE dbycql.customer_balance_top10;


-- create new tables at very first

-- 6. popular item
CREATE TABLE dbycql.orderline (
    ol_w_id int,
    ol_d_id int,
    ol_o_id int,
    ol_number int,
    ol_i_id int,
    ol_delivery_d timestamp,
    ol_amount decimal,
    ol_supply_w_id int,
    ol_quantity decimal,
    ol_dist_info text,
    PRIMARY KEY ((ol_w_id, ol_d_id), ol_quantity, ol_o_id, ol_number) -- change
) WITH CLUSTERING ORDER BY (ol_quantity DESC, ol_o_id ASC, ol_number ASC); -- change

copy dbycql.orderline (ol_w_id, ol_d_id, ol_o_id, ol_number, ol_i_id, ol_delivery_d, ol_amount, ol_supply_w_id, ol_quantity, ol_dist_info)
from '/home/stuproj/cs4224j/project_data/data_files/order-line.csv' 
WITH NULL='null' AND INGESTRATE=5000;

-- 7. top balance
CREATE TABLE dbycql.customer (
  C_W_id int,
  C_D_id int,
  C_id int,
  C_first varchar,
  C_middle varchar,
  C_last varchar,
  C_street_1 varchar,
  C_street_2 varchar,
  C_city varchar,
  C_state varchar,
  C_zip varchar,
  C_phone varchar,
  C_since timestamp,
  C_credit varchar,
  C_credit_lim decimal,
  C_discount decimal,
  C_balance decimal,
  C_ytd_payment DECIMAL,
  C_payment_cnt int,
  C_delivery_cnt int,
  C_data varchar,
  PRIMARY KEY ((C_W_ID, C_D_ID), C_BALANCE, C_ID))  -- change
WITH CLUSTERING ORDER BY (C_BALANCE DESC, C_ID ASC); -- change

copy customer (C_W_id,C_D_id,C_id,C_first,C_middle,C_last,C_street_1,C_street_2,C_city,C_state,C_zip,C_phone,C_since,C_credit,C_credit_lim,C_discount,C_balance,C_ytd_payment,C_payment_cnt,C_delivery_cnt,C_data)
from '/home/stuproj/cs4224j/project_data/data_files/customer.csv' 
WITH NULL='null' AND INGESTRATE=5000;
