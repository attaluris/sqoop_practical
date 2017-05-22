# this assumes that there are the two tables in the given database name
#!/bin/sh

usage() 
{ 
cat << EOF
usage: $0 options 
This script imports tables from the given database into hive. 
OPTIONS: 
-h Show this message 
-u mysql_user 
-p mysql_password
-d database_name
EOF
} 

# makes sure args are correct
USERN= 
PASSW= 
DATABASEN= 
while getopts “hu:p:d:” OPTION 
do 
case $OPTION in 
h) 
usage 
exit 1 
;; 
u) 
USERN=$OPTARG 
;; 
p) 
PASSW=$OPTARG 
;; 
d) 
DATABASEN=$OPTARG 
;; 
?) 
usage 
exit 
;;
esac 
done 
if [[ -z $USERN ]] || [[ -z $PASSW ]] || [[ -z $DATABASEN ]] 
then 
usage 
exit 1 
fi


# creates the databases
echo "create database"
hive -e "CREATE DATABASE IF NOT EXISTS ${DATABASEN};"

# clear existing data
hadoop fs -rm -r -skipTrash /user/$USER/user
hadoop fs -rm -r -skipTrash /user/$USER/activitylog
hadoop fs -rm -r -skipTrash /user/hive/warehouse/user
hadoop fs -rm -r -skipTrash /user/hive/warehouse/activitylog
hive -e "USE ${DATABASEN}; DROP TABLE IF EXISTS user; DROP TABLE IF EXISTS activitylog;"

# imports user from mysql
echo "import user"
sqoop import \
--connect jdbc:mysql://localhost/$DATABASEN \
--username $USERN \
--password $PASSW \
--table user \
-m 1 \
--hive-import \
--hive-database $DATABASEN \
--hive-table user

# imports activitylog from mysql
echo "import log"
sqoop import \
--connect jdbc:mysql://localhost/$DATABASEN \
--username $USERN \
--password $PASSW \
--table activitylog \
-m 1 \
--hive-import \
--hive-database $DATABASEN \
--hive-table activitylog
