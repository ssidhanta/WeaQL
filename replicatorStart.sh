#!/bin/bash
mv /etc/my"$i".cnf /etc/my.cnf
mv /etc/mysql/my"$i".cnf /etc/mysql/my.cnf
sudo mkdir /var/run/mysqld
#sudo mkdir /var/lib/mysql
sudo touch /var/run/mysqld/mysqld.sock
sudo chown -R mysql /var/run/mysqld
sudo chown -R mysql /var/lib/mysql
sudo usermod -d /var/lib/mysql/ mysql
#sudo /etc/init.d/mysql restart
sudo service mysql start
mysql -u root --default_character_set utf8 -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('101010');FLUSH PRIVILEGES;"
#sh /var/lib/serverStart.sh "$iparg"
#java -jar /var/lib/thesis_code-master/db-transform.jar "$iparg" "tpcc_crdt"
#java -jar /var/lib/thesis_code-master/tpcc-gendb.jar "$iparg" "tpcc_crdt" "$j"
#java -Dlog4j.configuration=file:"/var/lib/thesis_code-master/log4j_weakdb.properties" -jar /var/lib/thesis_code-master/replicator.jar /var/lib/thesis_code-master/docker_tpcc_7node.xml /var/lib/thesis_code-master/low_env_localhost_tpcc_default_coord.env "$k"
#sh /var/lib/serverStart1.sh "$iparg"
