SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

drop database if exists tpcc;
create database tpcc;
use tpcc;

drop table if exists warehouse;
drop table if exists district;
drop table if exists customer;
drop table if exists history;
drop table if exists new_orders;
drop table if exists orders;
drop table if exists order_line;
drop table if exists item;
drop table if exists stock;

commit;

create table warehouse (
w_id smallint not null,
w_name varchar(10), 
w_street_1 varchar(20), 
w_street_2 varchar(20), 
w_city varchar(20), 
w_state char(2), 
w_zip char(9), 
w_tax decimal(4,2), 
w_ytd decimal(12,2),
primary key (w_id) ) Engine=InnoDB;


create table district (
d_id tinyint not null, 
d_w_id smallint not null, 
d_name varchar(10), 
d_street_1 varchar(20), 
d_street_2 varchar(20), 
d_city varchar(20), 
d_state char(2), 
d_zip char(9), 
d_tax decimal(4,2), 
d_ytd decimal(12,2), 
d_next_o_id int,
primary key (d_w_id, d_id) ) Engine=InnoDB;


create table customer (
c_id int not null, 
c_d_id tinyint not null,
c_w_id smallint not null, 
c_first varchar(16), 
c_middle char(2), 
c_last varchar(16), 
c_street_1 varchar(20), 
c_street_2 varchar(20), 
c_city varchar(20), 
c_state char(2), 
c_zip char(9), 
c_phone char(16), 
c_since timestamp, 
c_credit char(2), 
c_credit_lim bigint, 
c_discount decimal(4,2), 
c_balance decimal(12,2), 
c_ytd_payment decimal(12,2), 
c_payment_cnt smallint, 
c_delivery_cnt smallint, 
c_data varchar(500),
PRIMARY KEY(c_w_id, c_d_id, c_id) ) Engine=InnoDB;


create table history (
h_c_id int, 
h_c_d_id tinyint, 
h_c_w_id smallint,
h_d_id tinyint,
h_w_id smallint,
h_date timestamp,
h_amount decimal(6,2), 
h_data varchar(24),
PRIMARY KEY(h_c_id, h_c_d_id, h_c_w_id,h_d_id,h_w_id,h_date) ) Engine=InnoDB;


create table new_orders (
no_o_id int not null,
no_d_id tinyint not null,
no_w_id smallint not null,
PRIMARY KEY(no_w_id, no_d_id, no_o_id)) Engine=InnoDB;


create table orders (
o_id int not null, 
o_d_id tinyint not null, 
o_w_id smallint not null,
o_c_id int,
o_entry_d timestamp,
o_carrier_id tinyint,
o_ol_cnt tinyint, 
o_all_local tinyint,
PRIMARY KEY(o_w_id, o_d_id, o_id) ) Engine=InnoDB;


create table order_line ( 
ol_o_id int not null, 
ol_d_id tinyint not null,
ol_w_id smallint not null,
ol_number tinyint not null,
ol_i_id int, 
ol_supply_w_id smallint,
ol_delivery_d timestamp, 
ol_quantity tinyint, 
ol_amount decimal(6,2), 
ol_dist_info char(24),
PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number) ) Engine=InnoDB ;


create table item (
i_id int not null, 
i_im_id int, 
i_name varchar(24), 
i_price decimal(5,2), 
i_data varchar(50),
PRIMARY KEY(i_id) ) Engine=InnoDB;


create table stock (
s_i_id int not null, 
s_w_id smallint not null, 
s_quantity smallint, 
s_dist_01 char(24), 
s_dist_02 char(24),
s_dist_03 char(24),
s_dist_04 char(24), 
s_dist_05 char(24), 
s_dist_06 char(24), 
s_dist_07 char(24), 
s_dist_08 char(24), 
s_dist_09 char(24), 
s_dist_10 char(24), 
s_ytd decimal(8,0), 
s_order_cnt smallint, 
s_remote_cnt smallint,
s_data varchar(50),
PRIMARY KEY(s_w_id, s_i_id) ) Engine=InnoDB ;

commit;

SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;

CREATE INDEX idx_customer ON customer (c_w_id,c_d_id,c_last,c_first);
CREATE INDEX idx_orders ON orders (o_w_id,o_d_id,o_c_id,o_id);
CREATE INDEX fkey_stock_2 ON stock (s_i_id);
CREATE INDEX fkey_order_line_2 ON order_line (ol_supply_w_id,ol_i_id);

ALTER TABLE district  ADD CONSTRAINT fkey_district_1 FOREIGN KEY(d_w_id) REFERENCES warehouse(w_id);
ALTER TABLE customer ADD CONSTRAINT fkey_customer_1 FOREIGN KEY(c_w_id,c_d_id) REFERENCES district(d_w_id,d_id);
ALTER TABLE history  ADD CONSTRAINT fkey_history_2 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(d_w_id,d_id);
ALTER TABLE new_orders ADD CONSTRAINT fkey_new_orders_1 FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES orders(o_w_id,o_d_id,o_id);
ALTER TABLE orders ADD CONSTRAINT fkey_orders_1 FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer(c_w_id,c_d_id,c_id);
ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_1 FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES orders(o_w_id,o_d_id,o_id);
ALTER TABLE stock ADD CONSTRAINT fkey_stock_1 FOREIGN KEY(s_w_id) REFERENCES warehouse(w_id);
ALTER TABLE stock ADD CONSTRAINT fkey_stock_2 FOREIGN KEY(s_i_id) REFERENCES item(i_id);


#NOTE: the following FKs are not shard-safe since they can reference a warehouse in another shard
#ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_2 FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock(s_w_id,s_i_id);
#ALTER TABLE history  ADD CONSTRAINT fkey_history_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer(c_w_id,c_d_id,c_id);


SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

commit;
