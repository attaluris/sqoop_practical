# needs to be with a (local) folder called user_upload which has the sole purpose of housing upload_dumps
#!/bin/bash

usage() 
{ 
cat << EOF 
usage: $0 options 
This script imports csv files from a folder into hive and HDFS. 
OPTIONS: 
-h Show this message 
-d database_name
EOF 
} 

DATABASE= 
while getopts “hd:” OPTION 
do 
case $OPTION in 
h) 
usage 
exit 1 
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
if [[ -z $DATABASE ]] 
then 
usage 
exit 1 
fi

hadoop fs -mkdir -p app/data/user_upload
hadoop fs -put user_upload/* app/data/user_upload

hive -e “CREATE DATABASE IF NOT EXISTS $DATABASE;”

hive -e “USE $DATABASE;
CREATE EXTERNAL TABLE user_uploads ( user_id INT, file_name STRING, timestamp BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION 'app/data/user_upload' TBLPROPERTIES ("skip.header.line.count"="1");”
