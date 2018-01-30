#!/bin/bash
#docker exec -it mysqlx1 #ssh -tt 192.168.0.13  
mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to 'sa'@'%' identified by '101010'"
mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to 'sa'@'$1' identified by '101010'"
mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to 'sa'@'localhost' identified by '101010'"
#docker exec -it mysqlx1 mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to 'sa'@'thesiscodemaster_mysqlload_1.thesiscodemaster_backend' identified by '101010'"
mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to 'root'@'%' identified by '101010'"
mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to 'root'@'localhost' identified by '101010'"
mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to 'root'@'$1' identified by '101010'"
#docker exec -it mysqlx1 mysql -uroot -p101010 -e "grant all privileges on *.* to 'root'@'thesiscodemaster_mysqlload_1.thesiscodemaster_backend' identified by '101010'"
mysql -uroot -p101010 --default_character_set utf8 -e "grant all privileges on *.* to ''@'localhost' identified by '101010'"
mysql -uroot --default_character_set utf8 -p101010 -e "grant all privileges on *.* to ''@'$1' identified by '101010'"
mysql -uroot -p101010 --default_character_set utf8  -e "grant all privileges on *.* to ''@'' identified by '101010'"
#docker exec -it mysqlx1 mysql -uroot -p101010 -e "grant all privileges on *.* to ''@'thesiscodemaster_mysqlload_1.thesiscodemaster_backend' identified by '101010'"
#mysql -uroot -p101010 -e "create database tpcc"
mysql -uroot -p101010 --default_character_set utf8 -e "SOURCE /var/lib/thesis_code-master/scripts/sql/create_tpcc.sql"
mysql -uroot -p101010 --default_character_set utf8 -e "SOURCE /var/lib/thesis_code-master/scripts/sql/create_tpcc_crdt.sql"
mysql -uroot -p101010 --default_character_set utf8 -e "SOURCE /var/lib/thesis_code-master/scripts/sql/create_crdt_tpcc.sql"
#mysql -uroot -p101010 --default_character_set utf8 -e "SOURCE /var/lib/thesis_code-master/scripts/sql/create_micro.sql"
#mysql -uroot -p101010 -e "SOURCE /var/lib/thesis_code-master/scripts/sql/create_micro_crdt.sql"
#java -Dlog4j.configuration=file:"/var/lib/thesis_code-master/log4j_zookeeper.properties" -jar /var/lib/thesis_code-master/zookeeper-server.jar /var/lib/thesis_code-master/zoo.cfg &
#java -Dlog4j.configuration=file:"/var/lib/thesis_code-master/log4j_weakdb.properties" -jar /var/lib/thesis_code-master/replicator.jar /var/lib/thesis_code-master/docker_tpcc_7node.xml /var/lib/thesis_code-master/env_localhost_micro_default.env 1
