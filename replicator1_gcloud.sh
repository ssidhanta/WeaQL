#!/bin/bash
#mv /etc/my.cnf /etc/my.cnf
mv /etc/my.cnf /etc/mysql/my.cnf
#mv /etc/my4.cnf /etc/my.cnf
#mv /etc/mysql/my4.cnf /etc/mysql/my.cnf
sudo mkdir /var/run/mysqld
#sudo mkdir /var/lib/mysql
sudo touch /var/run/mysqld/mysqld.sock
sudo chown -R mysql /var/run/mysqld
sudo chown -R mysql /var/lib/mysql
sudo usermod -d /var/lib/mysql/ mysql
sudo /etc/init.d/mysql restart
#sudo service mysql start
#mysql -u root -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('101010');FLUSH PRIVILEGES;"
#mysql -u root -e "SET PASSWORD FOR 'root'@'$iparg' = PASSWORD('101010');FLUSH PRIVILEGES;"
#sh /var/lib/serverStart1.sh "$iparg"
#sh /var/lib/serverStart1.sh "$j" "$iparg"
