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

CREATE TABLE customer (    
    c_id int not null, 
    c_d_id tinyint not null,
    c_w_id smallint not null, 
    c_first varchar(16), 
    c_middle varchar(2), 
    c_last varchar(16), 
    c_street_1 varchar(20), 
    c_street_2 varchar(20), 
    c_city varchar(20), 
    c_state varchar(2), 
    c_zip varchar(9), 
    c_phone varchar(16), 
    c_since datetime, 
    c_credit varchar(2), 
    c_credit_lim bigint, 
    c_discount decimal(4,2), 
    c_balance decimal(12,2), 
    c_ytd_payment decimal(12,2), 
    c_payment_cnt smallint, 
    c_delivery_cnt smallint, 
    c_data varchar(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);



CREATE TABLE district (    
    d_id tinyint not null, 
    d_w_id smallint not null, 
    d_name varchar(10), 
    d_street_1 varchar(20), 
    d_street_2 varchar(20), 
    d_city varchar(20), 
    d_state varchar(2), 
    d_zip varchar(9), 
    d_tax decimal(4,2), 
    d_ytd decimal(12,2), 
    d_next_o_id int,
    PRIMARY KEY (d_w_id, d_id)
);


CREATE TABLE history (
    h_c_id int, 
    h_c_d_id tinyint, 
    h_c_w_id smallint,
    h_d_id tinyint,
    h_w_id smallint,
    h_date datetime,
    h_amount decimal(6,2), 
    h_data varchar(24)
);

CREATE TABLE item (    
    i_id int not null, 
    i_im_id int, 
    i_name varchar(24), 
    i_price decimal(5,2), 
    i_data varchar(50),
    PRIMARY KEY (i_id)
);

CREATE TABLE new_orders (    
    no_o_id int not null,
    no_d_id tinyint not null,
    no_w_id smallint not null
);

CREATE TABLE order_line (    
    ol_o_id int not null, 
    ol_d_id tinyint not null,
    ol_w_id smallint not null,
    ol_number tinyint not null,
    ol_i_id int, 
    ol_supply_w_id smallint,
    ol_delivery_d datetime, 
    ol_quantity tinyint, 
    ol_amount decimal(6,2), 
    ol_dist_info varchar(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

CREATE TABLE orders (    
    o_id int not null, 
    o_d_id tinyint not null, 
    o_w_id smallint not null,
    o_c_id int,
    o_entry_d datetime,
    o_carrier_id tinyint,
    o_ol_cnt tinyint, 
    o_all_local tinyint
);

CREATE TABLE stock (    
    s_i_id int not null, 
    s_w_id smallint not null, 
    s_quantity smallint, 
    s_dist_01 varchar(24), 
    s_dist_02 varchar(24),
    s_dist_03 varchar(24),
    s_dist_04 varchar(24), 
    s_dist_05 varchar(24), 
    s_dist_06 varchar(24), 
    s_dist_07 varchar(24), 
    s_dist_08 varchar(24), 
    s_dist_09 varchar(24), 
    s_dist_10 varchar(24), 
    s_ytd decimal(8,0), 
    s_order_cnt smallint, 
    s_remote_cnt smallint,
    s_data varchar(50),
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE warehouse (    
    w_id smallint not null,
    w_name varchar(10), 
    w_street_1 varchar(20), 
    w_street_2 varchar(20), 
    w_city varchar(20), 
    w_state varchar(2), 
    w_zip varchar(9), 
    w_tax decimal(4,2), 
    w_ytd decimal(12,2),
    PRIMARY KEY (w_id)
);

commit;

CREATE INDEX ix_customer ON customer (c_w_id, c_d_id, c_id);
CREATE INDEX ix_order_line ON order_line (ol_i_id);
CREATE INDEX pk_orders ON orders (o_w_id, o_d_id, o_id);
CREATE INDEX ix_orders ON orders (o_w_id, o_d_id, o_c_id);
CREATE INDEX ix_new_orders ON new_orders (no_w_id, no_d_id, no_o_id);

commit;

