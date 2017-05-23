# needs to be with a (local) folder called user_upload which has the sole purpose of housing upload_dumps
# this will make a new folder for you called processed; so make sure a folder with that name doesn't exist
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
exit 1
fi

for f in *:*;
do mv -v "$f" $(echo "$f" | tr ':' '-');
if [ $? -eq 0 ];
then echo "got rid of colons"
else echo "could not get rid of colons"
fi
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
if [ $? -eq 0 ];
then echo "got rid of periods"
else echo "could not get rid of periods"
exit 1
fi

cd ..

# imports new files from local to HDFS
hadoop fs -mkdir -p /app/data/user_upload
if [ $? -eq 0 ];
then echo "HDFS user_upload exists"
else echo "could not create HDFS user_upload"
exit 1
fi
hadoop fs -put user_upload/* /app/data/user_upload
if [ $? -eq 0 ];
then echo "added new dumps"
else echo "could not add new dumps"
exit 1
fi

# imports new files to processed folder
mkdir -p processed
if [ $? -eq 0 ];
then echo "processed exists"
else echo "could not create processed"
exit 1
fi
mv user_upload/* processed
if [ $? -eq 0 ];
then echo "moved new dumps to processed"
else echo "could not move new dumps to processed"
exit 1
fi


# makes database
hive -e "CREATE DATABASE IF NOT EXISTS ${DATABASEN};"
if [ $? -eq 0 ];
then echo "created database"
else echo "could not create the database"
exit 1
fi

# makes new user_uploads table with data in HDFS folder
hive -e "USE ${DATABASEN};
CREATE EXTERNAL TABLE IF NOT EXISTS user_uploads (user_id INT, file_name STRING, timestamp BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/app/data/user_upload' TBLPROPERTIES ('skip.header.line.count'='1');"
if [ $? -eq 0 ];
then echo "made new user_uploads table"
else echo "could not make new user_uploads table"
exit 1
fi
