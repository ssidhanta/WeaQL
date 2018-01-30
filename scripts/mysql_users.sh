#!/bin/bash
# Mysql user creation script

bin/mysql --defaults-file=my.cnf -u root -p101010 -e "DROP USER sa;"
bin/mysql --defaults-file=my.cnf -u root -p101010 -e "CREATE USER 'sa'@'localhost' IDENTIFIED BY '101010';"
bin/mysql --defaults-file=my.cnf -u root -p101010 -e "CREATE USER 'sa'@'%' IDENTIFIED BY '101010';"
bin/mysql --defaults-file=my.cnf -u root -p101010 -e "GRANT ALL PRIVILEGES ON *.* TO 'sa'@'localhost' WITH GRANT OPTION;"
bin/mysql --defaults-file=my.cnf -u root -p101010 -e "GRANT ALL PRIVILEGES ON *.* TO 'sa'@'%' WITH GRANT OPTION;"
bin/mysql --defaults-file=my.cnf -u root -p101010 -e "FLUSH PRIVILEGES;"