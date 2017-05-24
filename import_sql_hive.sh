# this assumes that there are the two tables in the given database name
# assumes that the user has loaded up the metastore with "nohup sqoop metastore &"
# this also assumes that there is an existing mySQL password file on HDFS
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


# creates the database
hive -e "CREATE DATABASE IF NOT EXISTS ${DATABASEN};"
if [ $? -eq 0 ];
    then echo "created database"
    else echo "could not create database"
fi

# clear existing data
hadoop fs -rm -r -skipTrash /user/$USER/user
hadoop fs -rm -r -skipTrash /user/$USER/activitylog

# imports user from mysql
echo "import user"
sqoop import \
--connect jdbc:mysql://localhost/$DATABASEN \
--username $USERN \
--password-file $PASSW \
--table user \
-m 4 \
--hive-import \
--hive-overwrite \
--hive-database $DATABASEN \
--hive-table user
if [ $? -eq 0 ];
    then echo "imported user"
    else echo "could not import user"
    exit 1
fi

# finds if the job exists
numJobs=$(sqoop job \--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \--list | grep -c "practical_exercise_1.activitylog")
echo "$numJobs jobs exist with that name"

if [ $numJobs -eq 0 ];
    then
# make activitylog job from mysql
    sqoop job \
    --meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
    --create ${DATABASEN}.activitylog \
    -- import \
    --connect jdbc:mysql://localhost/$DATABASEN \
    --username $USERN \
    --password-file $PASSW \
    --table activitylog \
    -m 4 \
    --hive-import \
    --hive-database $DATABASEN \
    --hive-table activitylog \
    --incremental append \
    --check-column id \
    --last-value 0
    if [ $? -eq 0 ];
	then echo "made the activitylog job"
	else echo "could not make the activitylog job"
	exit 1
    fi
fi

#runs the job 
sqoop job \
--meta-connect jdbc:hsqldb:hsql://localhost:16000/sqoop \
--exec ${DATABASEN}.activitylog  
if [ $? -eq 0 ];
    then echo "finished the activitylog job"
    else echo "could not finish the activitylog job"
    exit 1
fi
