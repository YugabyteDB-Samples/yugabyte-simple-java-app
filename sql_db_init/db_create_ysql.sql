
DROP DATABASE IF EXISTS dbysql5424;
CREATE DATABASE dbysql5424;
-- show all tables
-- \dt;

-- USE dbysql5424J; MySQL 
\c dbysql5424;


--  5 entity tables --
DROP TABLE if EXISTS warehouse CASCADE;
CREATE TABLE warehouse (
  W_id int NOT NULL,
  W_name varchar(10) NOT NULL,
  W_street_1 varchar(20) NOT NULL,
  W_street_2 varchar(20) NOT NULL,
  W_city varchar(20) NOT NULL,
  W_state char(2) NOT NULL,
  W_zip char(9) NOT NULL,
  W_tax decimal(4,4) NOT NULL,
  W_ytd decimal(12,2) NOT NULL,
  
  PRIMARY KEY(W_id HASH) -- yugabyte distrbuted table sharding
);

-- insert from csv
\copy warehouse from '/Users/kennywu/Documents/NUScode/CS5424proj/distributedDatabase/data_files/warehouse.csv' WITH (FORMAT CSV, NULL 'null');


DROP TABLE if EXISTS district CASCADE;
CREATE TABLE district (
  -- D W ID is a foreign key that refers to warehouse table.
  D_W_id int NOT NULL REFERENCES warehouse(W_id),
  D_id int NOT NULL,
  -- Note: as compound foreign key
  PRIMARY KEY((D_W_id, D_id) HASH), -- yugabyte distrbuted table sharding

  D_name varchar(10) NOT NULL,
  D_street_1 varchar(20) NOT NULL,
  D_street_2 varchar(20) NOT NULL,
  D_city varchar(20) NOT NULL,
  D_state char(2) NOT NULL,
  D_zip char(9) NOT NULL,
  D_tax decimal(4,4) NOT NULL,
  D_ytd decimal(12,2) NOT NULL,
  D_next_O_id int NOT NULL
);

\copy district from '/Users/kennywu/Documents/NUScode/CS5424proj/distributedDatabase/data_files/district.csv' WITH (FORMAT CSV, NULL 'null');


DROP TABLE if EXISTS customer CASCADE;
CREATE TABLE customer (
  -- combined (C W ID, C D ID) is a foreign key that refers to district table.
  C_W_id int NOT NULL,
  C_D_id int NOT NULL,
  FOREIGN KEY (C_W_id, C_D_id) REFERENCES district(D_W_id, D_id),
  C_id int NOT NULL,
  -- Note: as compound foreign key
  PRIMARY KEY((C_W_id, C_D_id, C_id) HASH), -- yugabyte distrbuted table sharding
  
  C_first varchar(16) NOT NULL,
  C_middle char(2) NOT NULL,
  C_last varchar(16) NOT NULL,
  C_street_1 varchar(20) NOT NULL,
  C_street_2 varchar(20) NOT NULL,
  C_city varchar(20) NOT NULL,
  C_state char(2) NOT NULL,
  C_zip char(9) NOT NULL,
  C_phone char(16) NOT NULL,
  C_since timestamp NOT NULL,
  C_credit char(2) NOT NULL,
  C_credit_lim decimal(12,2) NOT NULL,
  C_discount decimal(5,4) NOT NULL,
  C_balance decimal(12,2) NOT NULL,
  C_ytd_payment float NOT NULL,
  C_payment_cnt int NOT NULL,
  C_delivery_cnt int NOT NULL,
  C_data varchar(500) NOT NULL);

-- insert from csv
\copy customer from '/Users/kennywu/Documents/NUScode/CS5424proj/distributedDatabase/data_files/customer.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_rows from customer;

-- Note: order is a keyword in SQL due to "order by"
DROP TABLE if EXISTS orders CASCADE;
CREATE TABLE orders (
  -- (O W ID, O D ID, O C ID) is a foreign key that refers to customer table.
  O_W_id int NOT NULL,
  O_D_id int NOT NULL,
  O_id int NOT NULL,
  PRIMARY KEY((O_W_id, O_D_id, O_id) HASH),
  O_C_id int NOT NULL,
  FOREIGN KEY (O_W_id, O_D_id, O_C_id) REFERENCES customer(C_W_id, C_D_id, C_id),
  -- Note: as compound foreign key
  UNIQUE(O_W_id, O_D_id, O_C_id),

  -- The range of O CARRIER ID is [1,10]: use smallint in pgsql(but small int is 16 bit in CQL, tinyint is 8)
  O_carrier_id smallint, -- data has lots of null
  O_OL_cnt decimal(2,0) NOT NULL,
  O_all_local decimal(1,0) NOT NULL,
  O_entry_d timestamp NOT NULL
);

\copy orders from '/Users/kennywu/Documents/NUScode/CS5424proj/distributedDatabase/data_files/order.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_rows from orders;

DROP TABLE if EXISTS item CASCADE;
CREATE TABLE item (
  I_id int NOT NULL,
  PRIMARY KEY(I_id HASH),
  I_name varchar(24) NOT NULL,
  I_tax decimal(5,2) NOT NULL,
  I_im_id int NOT NULL,
  I_data varchar(50) NOT NULL
);
-- insert from csv
\copy item from '/Users/kennywu/Documents/NUScode/CS5424proj/distributedDatabase/data_files/item.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_rows from item;

-- 2 relationship tables -- 
DROP TABLE if EXISTS orderline CASCADE;
CREATE TABLE orderline (
  -- (OL W ID, OL D ID, OL O ID) is a foreign key that refers to Order table. 
  -- OL I ID is a foreign key that refers to item table.
  OL_W_id int NOT NULL, 
  OL_D_id int NOT NULL, 
  OL_O_id int NOT NULL,
  FOREIGN KEY (OL_W_id, OL_D_id, OL_O_id) REFERENCES orders(O_W_id, O_D_id, O_id),
  OL_number int NOT NULL,
  -- PRIMARY KEY(OL_W_id, OL_D_id, OL_O_id, OL_number),
  PRIMARY KEY((OL_W_id, OL_D_id, OL_O_id, OL_number) HASH),
  OL_I_id int NOT NULL REFERENCES item(I_id),
  
  
  OL_delivery_D timestamp, -- data has lots of null
  OL_amount decimal(7,2) NOT NULL,
  OL_supply_W_id int NOT NULL,
  OL_quantity decimal(2,0) NOT NULL,
  OL_dist_info char(24) NOT NULL
);

\copy orderline from '/Users/kennywu/Documents/NUScode/CS5424proj/distributedDatabase/data_files/order-line.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_rows from "orderline";

DROP TABLE if EXISTS stock CASCADE;
CREATE TABLE stock (
  -- S I ID is a foreign key that refers to item table. 
  -- S W ID is a foreign key that refers to warehouse table.
  S_W_id int NOT NULL REFERENCES warehouse(W_id),
  S_I_id int NOT NULL REFERENCES item(I_id),
  PRIMARY KEY((S_W_id, S_I_id) HASH),
  
  S_quantity decimal(4,0) NOT NULL,
  S_ytd decimal(8,2) NOT NULL,
  S_order_cnt int NOT NULL,
  S_remote_cnt int NOT NULL,
  S_dist_01 char(24) NOT NULL,
  S_dist_02 char(24) NOT NULL,
  S_dist_03 char(24) NOT NULL,
  S_dist_04 char(24) NOT NULL,
  S_dist_05 char(24) NOT NULL,
  S_dist_06 char(24) NOT NULL,
  S_dist_07 char(24) NOT NULL,
  S_dist_08 char(24) NOT NULL,
  S_dist_09 char(24) NOT NULL,
  S_dist_10 char(24) NOT NULL,
  S_dist_data varchar(50) NOT NULL
);


\copy stock from '/Users/kennywu/Documents/NUScode/CS5424proj/distributedDatabase/data_files/stock.csv' WITH (FORMAT CSV, NULL 'null');
select count(*) as no_imported_rows from stock;

-- show all tables
\dt;

