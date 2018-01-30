#!/bin/bash

mysql --socket=/tmp/mysql.sock -u sa -p101010 < create_tables_tpcc_crdt.sql
mysql --socket=/tmp/mysql.sock -u sa -p101010 < create_tables_tpcc.sql