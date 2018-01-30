#!/bin/bash
#mysql -u root -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('101010');FLUSH PRIVILEGES;
#sudo service mysql start
#mysql -uroot -p101010 -e "grant all privileges on *.* to 'sa'@'$1' identified by '101010'"
#mysql -uroot -p101010 -e "grant all privileges on *.* to 'root'@'$1' identified by '101010'"
#mysql -uroot -p101010 -e "grant all privileges on *.* to ''@'$1' identified by '101010'"
#mysql -uroot -p101010 -h "$1" tpcc_crdt -e "grant all privileges on *.* to 'sa'@'$2' identified by '101010'"
#mysql -uroot -p101010 -h "$1" tpcc_crdt -e "grant all privileges on *.* to 'root'@'$2' identified by '101010'"
#mysql -uroot -p101010 -h "$1" tpcc_crdt -e "grant all privileges on *.* to ''@'$2' identified by '101010'"
#sudo java "-Xms2G" "-Xmx6G" -Dlog4j.configuration=file:"/var/lib/thesis_code-master/log4j_weakdb.properties" -jar /var/lib/thesis_code-master/tpcc-client.jar /var/lib/thesis_code-master/docker_tpcc_7node.xml /var/lib/thesis_code-master/low_env_localhost_tpcc_default_coord.env /var/lib/thesis_code-master/workload1 "$j" 1 5 crdt
#sudo java "-Xms2G" "-Xmx6G" -Dlog4j.configuration=file:"/var/lib/thesis_code-master/log4j_weakdb.properties" -jar /var/lib/thesis_code-master/tpcc-client.jar /var/lib/thesis_code-master/docker_tpcc_7node.xml /var/lib/thesis_code-master/low_env_localhost_tpcc_default_coord.env /var/lib/thesis_code-master/workload1 1 4 60 crdt
#find . -name '*.csv'
find . -name '*.csv' -exec cat {} \;
