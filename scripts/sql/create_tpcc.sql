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

--
-- TOC entry 16 (OID 17148)
-- Name: customer; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE cnflct_flgs (
    c_tbl_chld varchar(20),
    c_tbl_chld_pk varchar(20),
    c_tbl_flgs varchar(20)
) ENGINE=InnoDB;

CREATE TABLE customer (    
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
    c_since datetime, 
    c_credit char(2), 
    c_credit_lim bigint, 
    c_discount decimal(4,2), 
    c_balance decimal(12,2), 
    c_ytd_payment decimal(12,2), 
    c_payment_cnt smallint, 
    c_delivery_cnt smallint, 
    c_data varchar(500)
) ENGINE=InnoDB;


--
-- TOC entry 17 (OID 17158)
-- Name: district; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE district (    
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
    d_next_o_id int
) ENGINE=InnoDB;;

--
-- TOC entry 18 (OID 17165)
-- Name: history; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE history (
    h_c_id int, 
    h_c_d_id tinyint, 
    h_c_w_id smallint,
    h_d_id tinyint,
    h_w_id smallint,
    h_date datetime,
    h_amount decimal(6,2), 
    h_data varchar(24)
) ENGINE=InnoDB;;


--
-- TOC entry 19 (OID 17170)
-- Name: item; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE item (    
    i_id int not null, 
    i_im_id int, 
    i_name varchar(24), 
    i_price decimal(5,2), 
    i_data varchar(50)
) ENGINE=InnoDB;;


--
-- TOC entry 20 (OID 17177)
-- Name: new_orders; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE new_orders (    
    no_o_id int not null,
    no_d_id tinyint not null,
    no_w_id smallint not null
) ENGINE=InnoDB;;

--
-- TOC entry 21 (OID 17182)
-- Name: order_line; Type: TABLE; Schema: public; Owner: tpcc
--

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
    ol_dist_info char(24)
) ENGINE=InnoDB;;


--
-- TOC entry 22 (OID 17187)
-- Name: orders; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE orders (    
    o_id int not null, 
    o_d_id tinyint not null, 
    o_w_id smallint not null,
    o_c_id int,
    o_entry_d datetime,
    o_carrier_id tinyint,
    o_ol_cnt tinyint, 
    o_all_local tinyint
) ENGINE=InnoDB;;

--
-- TOC entry 23 (OID 17192)
-- Name: stock; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE stock (    
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
    s_data varchar(50)
) ENGINE=InnoDB;;


--
-- TOC entry 24 (OID 17199)
-- Name: warehouse; Type: TABLE; Schema: public; Owner: tpcc
--

CREATE TABLE warehouse (    
    w_id smallint not null,
    w_name varchar(10), 
    w_street_1 varchar(20), 
    w_street_2 varchar(20), 
    w_city varchar(20), 
    w_state char(2), 
    w_zip char(9), 
    w_tax decimal(4,2), 
    w_ytd decimal(12,2)
) ENGINE=InnoDB;;

commit;

--
-- TOC entry 27 (OID 1115795)
-- Name: pk_customer; Type: CONSTRAINT; Schema: public; Owner: tpcc
--

ALTER TABLE customer
    ADD CONSTRAINT pk_customer PRIMARY KEY (c_w_id, c_d_id, c_id);


--
-- TOC entry 29 (OID 1115798)
-- Name: pk_district; Type: CONSTRAINT; Schema: public; Owner: tpcc
--

ALTER TABLE district
    ADD CONSTRAINT pk_district PRIMARY KEY (d_w_id, d_id);


--
-- TOC entry 31 (OID 1115800)
-- Name: pk_item; Type: CONSTRAINT; Schema: public; Owner: tpcc
--

ALTER TABLE item
    ADD CONSTRAINT pk_item PRIMARY KEY (i_id);


--
-- TOC entry 34 (OID 1115802)
-- Name: pk_order_line; Type: CONSTRAINT; Schema: public; Owner: tpcc
--

ALTER TABLE order_line
    ADD CONSTRAINT pk_order_line PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number);


--
-- TOC entry 39 (OID 1115808)
-- Name: pk_stock; Type: CONSTRAINT; Schema: public; Owner: tpcc
--

ALTER TABLE stock
    ADD CONSTRAINT pk_stock PRIMARY KEY (s_w_id, s_i_id);


--
-- TOC entry 41 (OID 1115811)
-- Name: pk_warehouse; Type: CONSTRAINT; Schema: public; Owner: tpcc
--

ALTER TABLE warehouse
    ADD CONSTRAINT pk_warehouse PRIMARY KEY (w_id);

commit;



SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;


--
-- TOC entry 25 (OID 1115797)
-- Name: ix_customer; Type: INDEX; Schema: public; Owner: tpcc
--
CREATE INDEX ix_customer ON customer (c_w_id, c_d_id, c_id);
--
-- TOC entry 33 (OID 1115804)
-- Name: ix_order_line; Type: INDEX; Schema: public; Owner: tpcc
--
CREATE INDEX ix_order_line ON order_line (ol_i_id);
--
-- TOC entry 36 (OID 1115805)
-- Name: pk_orders; Type: INDEX; Schema: public; Owner: tpcc
--
CREATE INDEX pk_orders ON orders (o_w_id, o_d_id, o_id);
--
-- TOC entry 35 (OID 1115806)
-- Name: ix_orders; Type: INDEX; Schema: public; Owner: tpcc
--
CREATE INDEX ix_orders ON orders (o_w_id, o_d_id, o_c_id);
--
-- TOC entry 32 (OID 1115807)
-- Name: ix_new_order; Type: INDEX; Schema: public; Owner: tpcc
--
CREATE INDEX ix_new_orders ON new_orders (no_w_id, no_d_id, no_o_id);
--
-- TOC entry 37 (OID 1115810)
-- Name: ix_stock; Type: INDEX; Schema: public; Owner: tpcc
--
-- CREATE INDEX ix_stock ON stock (s_i_id);

commit;

ALTER TABLE district ADD CONSTRAINT fkey_district_1 FOREIGN KEY(d_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
ALTER TABLE customer ADD CONSTRAINT fkey_customer_1 FOREIGN KEY(c_w_id,c_d_id) REFERENCES district(d_w_id,d_id) ON DELETE CASCADE;
ALTER TABLE history ADD CONSTRAINT fkey_history_2 FOREIGN KEY(h_w_id,h_d_id) REFERENCES district(d_w_id,d_id) ON DELETE CASCADE;
ALTER TABLE new_orders ADD CONSTRAINT fkey_new_orders_1 FOREIGN KEY(no_w_id,no_d_id,no_o_id) REFERENCES orders(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
ALTER TABLE orders ADD CONSTRAINT fkey_orders_1 FOREIGN KEY(o_w_id,o_d_id,o_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;
ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_1 FOREIGN KEY(ol_w_id,ol_d_id,ol_o_id) REFERENCES orders(o_w_id,o_d_id,o_id) ON DELETE CASCADE;
ALTER TABLE stock ADD CONSTRAINT fkey_stock_1 FOREIGN KEY(s_w_id) REFERENCES warehouse(w_id) ON DELETE CASCADE;
ALTER TABLE stock ADD CONSTRAINT fkey_stock_2 FOREIGN KEY(s_i_id) REFERENCES item(i_id) ON DELETE CASCADE;

-- ALTER TABLE order_line ADD CONSTRAINT fkey_order_line_2 FOREIGN KEY(ol_supply_w_id,ol_i_id) REFERENCES stock(s_w_id,s_i_id) ON DELETE CASCADE;
-- ALTER TABLE history ADD CONSTRAINT fkey_history_1 FOREIGN KEY(h_c_w_id,h_c_d_id,h_c_id) REFERENCES customer(c_w_id,c_d_id,c_id) ON DELETE CASCADE;


SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;

commit;

