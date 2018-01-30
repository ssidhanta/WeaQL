#!/bin/bash
# waitformysql.sh

set -e

host="$1"
shift
cmd="$@"

# Returns true once mysql can connect.
mysql_ready() {
   mysqladmin ping --host=192.168.0.1 --user=root --password=101010 > /dev/null 2>&1
}

service mysql start
while !(mysql_ready)
do #mysql -h "$host" -uroot -p101010 -c '\l'; do
  >&2 echo "Mysql is unavailable - sleeping"
  sleep 1
done

>&2 echo "Mysql is up - executing command"
exec $cmd
