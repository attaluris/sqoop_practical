# this assumes that there are the two tables in the given database name
#!/bin/bash

usage() 
{ 
cat << EOF
usage: $0 options 
This script imports tables from the given database into hive. 
OPTIONS: 
-h Show this message 
-u mysql_user 
-p mysql_password_file_location 
-d database_name
EOF
} 

USER= 
PASS= 
DATABASE= 
while getopts “hu:p:d:” OPTION 
do 
case $OPTION in 
h) 
usage 
exit 1 
;; 
u) 
USER=$OPTARG 
;; 
p) 
PASS=$OPTARG 
;; 
d) 
DATABASE=$OPTARG 
;; 
?) 
usage 
exit 
;; 
esac 
done 
if [[ -z $USER ]] || [[ -z $PASS ]] || [[ -z $DATABASE ]] 
then 
usage 
exit 1 
fi

hive -e “CREATE DATABASE IF NOT EXISTS $DATABASE;”

# puts them in HDBC
sqoop import \ 
--connect jdbc:mysql://localhost/$DATABASE \ 
--username $USER \ 
--password-file $PASS \ 
--table user \ 
-m 1 \ 
--target-dir /user/hive/warehouse/user

sqoop import \ 
--connect jdbc:mysql://localhost/$DATABASE \ 
--username $USER \ 
--password-file $PASS \ 
--table activitylog \ 
--target-dir /user/hive/warehouse/activitylog

# puts them in Hive
hadoop fs -rm -r -skipTrash /user/$USER/user

sqoop import \ 
--connect jdbc:mysql://localhost/$DATABASE \ 
--username $USER \ 
--password-file $PASS \ 
--table user \ 
-m 1 \ 
--hive-import \ 
--hive-database $DATABASE \ 
--hive-table user

hadoop fs -rm -r -skipTrash /user/$USER/activitylog

sqoop import \ 
--connect jdbc:mysql://localhost/$DATABASE \ 
--username $USER \ 
--password-file $PASS \ 
--table activitylog \ 
-m 1 \ 
--hive-import \ 
--hive-database $DATABASE \ 
--hive-table activitylog
