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
-p mysql_password_location
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
hive -e "CREATE DATABASE IF NOT EXISTS ${DATABASEN};"
if [ $? -eq 0 ];
then echo "created database"
else echo "could not create database"
fi

# imports the password from local to HDFS
hadoop fs -mkdir -p /user/$USER
if [ $? -eq 0 ];
then echo "hadoop password directory exists"
else echo "could not create password directory"
fi
hadoop fs -rm -r /user/$USER/password.txt
if [ $? -eq 0 ];
then echo "removed old password file"
else echo "could not remove old password file"
fi
hadoop fs -put $PASSW /user/$USER/password.txt
if [ $? -eq 0 ];
then echo "copied password file"
else echo "could not copy password file"
exit
fi

# clear existing data
hadoop fs -rm -r -skipTrash /user/$USER/user
hadoop fs -rm -r -skipTrash /user/$USER/activitylog
hadoop fs -rm -r -skipTrash /user/hive/warehouse/user
hadoop fs -rm -r -skipTrash /user/hive/warehouse/activitylog
hive -e "USE ${DATABASEN}; DROP TABLE IF EXISTS user; DROP TABLE IF EXISTS activitylog;"
if [ $? -eq 0 ];
then echo "deleted old tables"
else echo "could not delete old tables"
exit
fi


# imports user from mysql
echo "import user"
sqoop import \
--connect jdbc:mysql://localhost/$DATABASEN \
--username $USERN \
--password-file /user/$USER/password.txt \
--table user \
-m 1 \
--hive-import \
--hive-database $DATABASEN \
--hive-table user
if [ $? -eq 0 ];
then echo "imported user"
else echo "could not import user"
exit
fi

# imports activitylog from mysql
echo "import log"
sqoop import \
--connect jdbc:mysql://localhost/$DATABASEN \
--username $USERN \
--password-file /user/$USER/password.txt \
--table activitylog \
-m 1 \
--hive-import \
--hive-database $DATABASEN \
--hive-table activitylog
if [ $? -eq 0 ];
then echo "imported activitylog"
else echo "could not import activitylog"
exit
fi
