
DROP KEYSPACE IF EXISTS dbycql;

-- data path: /temp/project_data/data_files/

-- show all key spaces(DBs)
CREATE KEYSPACE dbycql 
WITH REPLICATION = {
  'class': 'SimpleStrategy', 
  'replication_factor': '1'} 
  ;
  AND DURABLE_WRITES = true;

-- DESCRIBE dbycql;
USE dbycql;


-- cql支持，键空间是一个定义节点上数据复制的命名空间。集群每个节点包含一个键空间，是否可以优化
-- cql的type, varchar == text
-- 似乎没有char

--  5 entity tables --
drop table if exists warehouse;
CREATE TABLE warehouse (
  -- 1-10, so ok in tinyint for [-128	127]
  W_id int, 
  -- partition key and primary key
  PRIMARY KEY(W_id), 
  W_name varchar,
  W_street_1 varchar,
  W_street_2 varchar,
  W_city varchar,
  W_state varchar,
  W_zip varchar,
  W_tax decimal,
  W_ytd decimal
);

-- insert from csv
-- WITH NULL='null': set NULL='null' or whatever character you want for null value ...
copy warehouse from '/temp/project_data/data_files/warehouse.csv'
WITH NULL='null';

drop table if exists district;
CREATE TABLE district (
  -- 1-10,
  D_W_id int,
  -- 1-10,
  D_id int,
  -- Note: D_id is clustering key
  PRIMARY KEY(D_W_id, D_id),

  D_name varchar,
  D_street_1 varchar,
  D_street_2 varchar,
  D_city varchar,
  D_state varchar,
  D_zip varchar,
  D_tax decimal,
  D_ytd decimal,
  D_next_O_id int
);

copy district from '/temp/project_data/data_files/district.csv' 
WITH NULL='null';

drop table if exists customer;
CREATE TABLE customer (
  -- combined (C W ID, C D ID) is a foreign key that refers to district table.
  C_W_id int,
  C_D_id int,
  C_id int,
  -- Note: as compound foreign key
  PRIMARY KEY((C_W_id, C_D_id), C_id),
  
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
  C_ytd_payment float,
  C_payment_cnt int,
  C_delivery_cnt int,
  C_data varchar);

-- insert from csv
COPY customer from '/temp/project_data/data_files/customer.csv'
WITH NULL='null';
select count(*) as no_imported_rows from customer;

-- Note: order is a keyword in SQL due to "order by"
drop table if exists orders;
CREATE TABLE orders (
  -- (O W ID, O D ID, O C ID) is a foreign key that refers to customer table.
  O_W_id int,
  O_D_id int,
  O_id int,
  O_C_id int,
  PRIMARY KEY ((O_W_id, O_D_id), O_C_id),
  -- Note: as compound foreign key

  -- The range of O CARRIER ID is [1,10]: use smallint is 16 bit in CQL, tinyint is 8
  O_carrier_id tinyint, -- data has lots of null
  O_OL_cnt decimal,
  O_all_local decimal,
  O_entry_d timestamp
);

COPY orders from '/temp/project_data/data_files/order.csv'
WITH NULL='null';
select count(*) as row_imported_orders from orders where;
-- select count(*) from orders where O_carrier_id == 'null';


drop table if exists item;
CREATE TABLE item (
  I_id int PRIMARY KEY,
  I_name varchar,
  I_tax decimal,
  I_im_id int,
  I_data varchar
);
-- insert from csv
COPY item from '/temp/project_data/data_files/item.csv'
WITH NULL='null';
select count(*) as no_imported_rows from item;

-- 2 relationship tables -- 
drop table if exists order_line;
CREATE TABLE order_line (
  -- OL I ID is a foreign key that refers to item table.
  OL_W_id int, 
  OL_D_id int, 
  OL_O_id int,
  OL_number int,
  PRIMARY KEY((OL_W_id, OL_D_id, OL_O_id), OL_number),
  OL_I_id int,
  OL_delivery_D timestamp, -- data has lots of null
  OL_amount decimal,
  OL_supply_W_id int,
  OL_quantity decimal,
  OL_dist_info varchar
);

COPY order_line from '/temp/project_data/data_files/order-line.csv'
WITH NULL='null';
-- select * from "order_line" where OL_delivery_D is null;
select count(*) as no_imported_rows from order_line;


drop table if exists stock;
CREATE TABLE stock (
  -- S I ID is a primary key that refers to item table. 
  -- S W ID is a cluster key that refers to warehouse table.
  S_W_id int,
  S_I_id int,
  PRIMARY KEY(S_W_id, S_I_id),
  
  S_quantity decimal,
  S_ytd decimal,
  S_order_cnt int,
  S_remote_cnt int,
  S_dist_01 varchar,
  S_dist_02 varchar,
  S_dist_03 varchar,
  S_dist_04 varchar,
  S_dist_05 varchar,
  S_dist_06 varchar,
  S_dist_07 varchar,
  S_dist_08 varchar,
  S_dist_09 varchar,
  S_dist_10 varchar,
  S_dist_data varchar
);


COPY stock from '/temp/project_data/data_files/stock.csv'
WITH NULL='null';
select count(*) as no_imported_rows from stock;

-- show all tables
DESCRIBE tables;
