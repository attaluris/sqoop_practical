# needs to be with a (local) folder called user_upload which has the sole purpose of housing upload_dumps
#!/bin/sh

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

# makes sure that the args are correct
DATABASEN= 
while getopts “hd:” OPTION 
do 
case $OPTION in 
h) 
usage 
exit 1 
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
if [[ -z $DATABASEN ]] 
then 
usage 
exit 1 
fi

# corrects file names for colons and periods
cd user_upload
for f in *:*;
do mv -v "$f" $(echo "$f" | tr ':' '-');
done
for fname in *; do
  name="${fname%\.*}"
  extension="${fname#$name}"
  newname="${name//./_}"
  newfname="$newname""$extension"
  if [ "$fname" != "$newfname" ]; then
    echo mv "$fname" "$newfname"
    mv "$fname" "$newfname"
  fi
done
cd ..

# imports from local to HDFS
hadoop fs -mkdir -p /app/data/user_upload
hadoop fs -put user_upload/* /app/data/user_upload

# makes database
hive -e "CREATE DATABASE IF NOT EXISTS ${DATABASEN};"

# clears old table
hadoop fs -rm -r -skipTrash /user/$USER/user_upload

# makes new user_uploads table with data in HDFS folder
hive -e "USE ${DATABASEN};
DROP TABLE IF EXISTS user_uploads;
CREATE EXTERNAL TABLE user_uploads (user_id INT, file_name STRING, timestamp BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/app/data/user_upload' TBLPROPERTIES ('skip.header.line.count'='1');"
