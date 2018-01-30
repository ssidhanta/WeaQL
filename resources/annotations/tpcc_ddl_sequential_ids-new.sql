@AW create table warehouse (
	@LWWINTEGER w_id smallint not null,
	@NORMALSTRING w_name varchar(10), 
	@NORMALSTRING w_street_1 varchar(20), 
	@NORMALSTRING w_street_2 varchar(20), 
	@NORMALSTRING w_city varchar(20), 
	@NORMALSTRING w_state char(2), 
	@NORMALSTRING w_zip char(9), 
	@LWWINTEGER w_tax decimal(4,2), 
	@NUMDELTAINTEGER w_ytd decimal(12,2),
	PRIMARY KEY (w_id) 
)Engine=InnoDB;

@RW create table district (
	@LWWINTEGER d_id tinyint not null, 
	@LWWINTEGER d_w_id smallint not null, 
	@NORMALSTRING d_name varchar(10), 
	@NORMALSTRING d_street_1 varchar(20), 
	@NORMALSTRING d_street_2 varchar(20), 
	@NORMALSTRING d_city varchar(20), 
	@NORMALSTRING d_state char(2), 
	@NORMALSTRING d_zip char(9), 
	@LWWINTEGER d_tax decimal(4,2), 
	@NUMDELTAINTEGER d_ytd decimal(12,2), 
	@NUMDELTAINTEGER d_next_o_id int,
	PRIMARY KEY (d_w_id, d_id),
	FOREIGN KEY(d_w_id) @FR REFERENCES warehouse(w_id)
)Engine=InnoDB;

@RW create table customer (
	@LWWINTEGER c_id int not null, 
	@LWWINTEGER c_d_id tinyint not null,
	@LWWINTEGER c_w_id smallint not null, 
	@LWWSTRING c_first varchar(16), 
	@LWWSTRING c_middle char(2), 
	@LWWSTRING c_last varchar(16), 
	@LWWSTRING c_street_1 varchar(20), 
	@LWWSTRING c_street_2 varchar(20), 
	@LWWSTRING c_city varchar(20), 
	@LWWSTRING c_state char(2), 
	@LWWSTRING c_zip char(9), 
	@LWWSTRING c_phone char(16), 
	@LWWDATETIME c_since date, 
	@LWWSTRING c_credit char(2), 
	@LWWINTEGER c_credit_lim bigint, 
	@LWWINTEGER c_discount decimal(12,2),
	@NUMDELTAINTEGER c_balance decimal(12,2), 
	@LWWINTEGER c_ytd_payment decimal(12,2), 
	@LWWINTEGER c_payment_cnt smallint, 
	@NUMDELTAINTEGER c_delivery_cnt smallint, 
	@LWWSTRING c_data varchar(500),
	PRIMARY KEY(c_w_id, c_d_id, c_id),
	FOREIGN KEY(c_w_id,c_d_id) @FR REFERENCES district(d_w_id,d_id)
)Engine=InnoDB;

@AW create table history (
	@LWWINTEGER @NOSEMANTIC h_id int AUTO_INCREMENT not null,
	@LWWINTEGER h_c_id int, 
	@LWWINTEGER h_c_d_id tinyint, 
	@LWWINTEGER h_c_w_id smallint,
	@LWWINTEGER h_d_id tinyint,
	@LWWINTEGER h_w_id smallint,
	@LWWDATETIME h_date date,
	@LWWINTEGER h_amount decimal(6,2), 
	@NORMALSTRING h_data varchar(24),
	PRIMARY KEY(h_id),
	FOREIGN KEY(h_w_id,h_d_id) @IR REFERENCES district(d_w_id,d_id)
)Engine=InnoDB;

@RW create table orders (
	@LWWINTEGER @SEMANTIC o_id int AUTO_INCREMENT not null,
	@LWWINTEGER o_d_id tinyint not null, 
	@LWWINTEGER o_w_id smallint not null,
	@LWWINTEGER o_c_id int,
	@LWWDATETIME o_entry_d date,
	@LWWINTEGER o_carrier_id tinyint DEFAULT null,
	@LWWINTEGER o_ol_cnt tinyint, 
	@LWWINTEGER o_all_local tinyint,
	PRIMARY KEY(o_w_id, o_d_id, o_id),
	FOREIGN KEY(o_w_id,o_d_id,o_c_id) @FR REFERENCES customer(c_w_id,c_d_id,c_id)
)Engine=InnoDB;

@AW create table new_orders (
	@LWWINTEGER @NOSEMANTIC no_o_id int AUTO_INCREMENT not null,
	@LWWINTEGER no_d_id tinyint not null,
	@LWWINTEGER no_w_id smallint not null,
	PRIMARY KEY(no_w_id, no_d_id, no_o_id),
	FOREIGN KEY(no_w_id,no_d_id,no_o_id) @IR REFERENCES orders(o_w_id,o_d_id,o_id)	
)Engine=InnoDB;

@RW create table item (
	@LWWINTEGER i_id int not null, 
	@LWWINTEGER i_im_id int, 
	@LWWSTRING i_name varchar(24), 
	@LWWINTEGER i_price decimal(5,2), 
	@LWWSTRING i_data varchar(50),
	PRIMARY KEY(i_id)
)Engine=InnoDB;

@AW create table stock (
	@LWWINTEGER s_i_id int not null, 
	@LWWINTEGER s_w_id smallint not null, 
	@LWWINTEGER s_quantity smallint, 
	@LWWSTRING s_dist_01 char(24), 
	@LWWSTRING s_dist_02 char(24),
	@LWWSTRING s_dist_03 char(24),
	@LWWSTRING s_dist_04 char(24), 
	@LWWSTRING s_dist_05 char(24), 
	@LWWSTRING s_dist_06 char(24), 
	@LWWSTRING s_dist_07 char(24), 
	@LWWSTRING s_dist_08 char(24), 
	@LWWSTRING s_dist_09 char(24), 
	@LWWSTRING s_dist_10 char(24), 
	@LWWINTEGER s_ytd decimal(8,0), 
	@LWWINTEGER s_order_cnt smallint, 
	@LWWINTEGER s_remote_cnt smallint,
	@LWWSTRING s_data varchar(50),
	PRIMARY KEY(s_w_id, s_i_id),
	FOREIGN KEY(s_w_id) @IR REFERENCES warehouse(w_id),
	FOREIGN KEY(s_i_id) @FR REFERENCES item(i_id)
)Engine=InnoDB;

@RW create table order_line (
	@LWWINTEGER @NOSEMANTIC ol_o_id int AUTO_INCREMENT not null,
	@LWWINTEGER ol_d_id tinyint not null,
	@LWWINTEGER ol_w_id smallint not null,
	@LWWINTEGER ol_number tinyint not null,
	@LWWINTEGER ol_i_id int, 
	@LWWINTEGER ol_supply_w_id smallint,
	@LWWDATETIME ol_delivery_d date DEFAULT NULL, 
	@LWWINTEGER ol_quantity tinyint, 
	@LWWINTEGER ol_amount decimal(6,2), 
	@LWWSTRING ol_dist_info char(24),
	PRIMARY KEY(ol_w_id, ol_d_id, ol_o_id, ol_number),
	FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) @FR REFERENCES orders(o_w_id,o_d_id,o_id)
)Engine=InnoDB;
