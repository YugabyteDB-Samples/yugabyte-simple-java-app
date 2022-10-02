
DROP DATABASE IF EXISTS db5424;
CREATE DATABASE db5424; 
USE db5424; 

--  5 entity tables --
CREATE TABLE Warehouse (
  W_id INT NOT NULL PRIMARY KEY,
  W_name varchar(10) NOT NULL,
  W_street_1 varchar(20) NOT NULL,
  W_street_2 varchar(20) NOT NULL,
  W_city varchar(20) NOT NULL,
  W_state char(2) NOT NULL,
  W_zip char(9) NOT NULL,
  W_tax decimal(4,4) NOT NULL,
  W_ytd decimal(12,2) NOT NULL
);


CREATE TABLE District (
  -- D W ID is a foreign key that refers to Warehouse table.
  D_W_id INT NOT NULL,
  FOREIGN KEY (D_W_id) REFERENCES Warehouse(W_id),
  D_id INT NOT NULL PRIMARY KEY,
  
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

CREATE TABLE Customer (
  -- (C W ID, C D ID) is a foreign key that refers to District table.
  C_W_id INT NOT NULL, 
  FOREIGN KEY (C_W_id) REFERENCES District(D_W_id),
  C_D_id INT NOT NULL,
  FOREIGN KEY (C_D_id) REFERENCES District(D_id),
  C_id INT NOT NULL PRIMARY KEY,
  
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
  -- 大，意思可以缩小些
  C_data varchar(500) NOT NULL
);

CREATE TABLE `Order` (
  -- (O W ID, O D ID, O C ID) is a foreign key that refers to Customer table.
  O_W_id INT NOT NULL,
  FOREIGN KEY (O_W_id) REFERENCES Customer(C_W_id),
  O_D_id INT NOT NULL,
  FOREIGN KEY (O_D_id) REFERENCES Customer(C_D_id),
  O_id INT NOT NULL PRIMARY KEY,
  O_C_id int NOT NULL,
  FOREIGN KEY (O_C_id) REFERENCES Customer(C_id),
  -- The range of O CARRIER ID is [1,10].
  O_Carrier_id int NOT NULL,
  O_OL_cnt decimal(2,0) NOT NULL comment "Number of items ordered",
  O_all_local decimal(1,0) NOT NULL comment "Order status (whether order includes only home order-lines)",
  O_entry_d timestamp NOT NULL comment "Order entry data and time"
);

CREATE TABLE `Item` (
  I_id INT NOT NULL PRIMARY KEY,
  I_name varchar(24) NOT NULL,
  I_tax decimal(5,2) NOT NULL,
  I_im_id int NOT NULL,
  I_data varchar(50) NOT NULL
);

-- 2 relationship tables -- 
CREATE TABLE `Order-Line` (
  -- (OL W ID, OL D ID, OL O ID) is a foreign key that refers to Order table. 
  -- OL I ID is a foreign key that refers to Item table.
  OL_W_id INT NOT NULL, 
  FOREIGN KEY (OL_W_id) REFERENCES `Order`(O_W_id),
  OL_D_id INT NOT NULL, 
  FOREIGN KEY (OL_D_id) REFERENCES `Order`(O_D_id),
  OL_O_id int NOT NULL,
  FOREIGN KEY (OL_O_id) REFERENCES `Order`(O_id),
  OL_number INT NOT NULL PRIMARY KEY,
  OL_I_id INT NOT NULL, 
  FOREIGN KEY (OL_I_id) REFERENCES Item(I_id),
  
  OL_delivery_D timestamp NOT NULL,
  OL_amount decimal(7,2) NOT NULL,
  OL_supply_W_id int NOT NULL,
  OL_quantity decimal(2,0) NOT NULL,
  OL_dist_info char(4) NOT NULL
);

CREATE TABLE Stock (
  -- S I ID is a foreign key that refers to Item table. 
  -- S W ID is a foreign key that refers to Warehouse table.
  S_W_id INT NOT NULL,
  FOREIGN KEY (S_W_id) REFERENCES Warehouse(W_id),
  S_I_id int NOT NULL,
  FOREIGN KEY (S_I_id) REFERENCES Item(I_id),
  
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
