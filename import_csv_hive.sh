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
if [ $? -eq 0 ];
then echo "entered user_upload"
else echo "could not enter user_upload"
exit
fi
for f in *:*;
do mv -v "$f" $(echo "$f" | tr ':' '-');
done
if [ $? -eq 0 ];
then echo "got rid of colons"
else echo "could not get rid of colons"
fi
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
if [ $? -eq 0 ];
then echo "got rid of periods"
else echo "could not get rid of periods"
exit
fi
cd ..

# imports from local to HDFS
hadoop fs -mkdir -p /app/data/user_upload
if [ $? -eq 0 ];
then echo "HDFS user_upload exists"
else echo "could not creat HDFS user_upload"
exit
fi
hadoop fs -put user_upload/* /app/data/user_upload
if [ $? -eq 0 ];
then echo "added new dumps"
else echo "could not add new dumps"
fi

# makes database
hive -e "CREATE DATABASE IF NOT EXISTS ${DATABASEN};"
if [ $? -eq 0 ];
then echo "created database"
else echo "could not create the database"
exit
fi

# clears old table
hadoop fs -rm -r -skipTrash /user/$USER/user_upload
if [ $? -eq 0 ];
then echo "cleared old table"
else echo "could not clear old table"
fi

# makes new user_uploads table with data in HDFS folder
hive -e "USE ${DATABASEN};
DROP TABLE IF EXISTS user_uploads;
CREATE EXTERNAL TABLE user_uploads (user_id INT, file_name STRING, timestamp BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/app/data/user_upload' TBLPROPERTIES ('skip.header.line.count'='1');"
if [ $? -eq 0 ];
then echo "made new user_uploads table"
else echo "could not make new user_uploads table"
exit
fi
