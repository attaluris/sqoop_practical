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

# makes sures args are correct
DATABASEN= 
TIME=$(date +"%s")
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
usage I
exit 1 
fi

# clears user_report
hive -e "USE ${DATABASEN}; DROP TABLE IF EXISTS user_report; DROP TABLE IF EXISTS active_user_report;"
if [ $? -eq 0 ];
then echo "deleted old user reports"
else echo "could not delete old user reports"
fi

# Robert’s user_report commands
hive -e "USE ${DATABASEN};
CREATE TABLE IF NOT EXISTS user_report AS SELECT
u.id,
CASE WHEN a_update.total_updates IS NULL THEN 0 ELSE a_update.total_updates END AS total_updates,
CASE WHEN a_insert.total_inserts IS NULL THEN 0 ELSE a_insert.total_inserts END AS total_inserts,
CASE WHEN a_delete.total_deletes IS NULL THEN 0 ELSE a_delete.total_deletes END AS total_deletes,
a_last_activity.last_activity_type,
CASE WHEN a_is_active.is_active IS NULL THEN false ELSE a_is_active.is_active END AS is_active,
CASE WHEN a_upload_count.upload_count IS NULL THEN 0 ELSE a_upload_count.upload_count END AS upload_count
FROM user AS u 
LEFT JOIN (SELECT user_id, count(*) total_updates FROM activitylog WHERE type = 'UPDATE' GROUP BY user_id) AS a_update ON u.id = a_update.user_id
LEFT JOIN (SELECT user_id, count(*) total_inserts FROM activitylog WHERE type = 'INSERT' GROUP BY user_id) AS a_insert ON u.id = a_insert.user_id
LEFT JOIN (SELECT user_id, count(*) total_deletes FROM activitylog WHERE type = 'DELETE' GROUP BY user_id) AS a_delete ON u.id = a_delete.user_id
LEFT JOIN (SELECT a1.user_id, a1.type AS last_activity_type FROM activitylog AS a1
JOIN (SELECT user_id, max(timestamp) AS last_activity_time FROM activitylog GROUP BY user_id) AS a2 ON a1.user_id = a2.user_id AND a1.timestamp = a2.last_activity_time) AS a_last_activity ON u.id = a_last_activity.user_id
LEFT JOIN (SELECT user_id, (($TIME - max(timestamp)) < 172800) AS is_active FROM activitylog GROUP BY user_id) AS a_is_active ON u.id = a_is_active.user_id
LEFT JOIN (SELECT user_id, count(*) AS upload_count FROM user_uploads GROUP BY user_id) AS a_upload_count ON u.id = a_upload_count.user_id
ORDER BY u.id ASC;"
if [ $? -eq 0 ];
then echo "made new user_report"
else echo "could not make new user_report"
exit
fi

# active_user_report commands
hive -e "USE ${DATABASEN};
CREATE TABLE IF NOT EXISTS active_user_report AS SELECT
ur.id,
CASE WHEN a_update.total_updates IS NULL THEN 0 ELSE a_update.total_updates END AS total_updates,
CASE WHEN a_insert.total_inserts IS NULL THEN 0 ELSE a_insert.total_inserts END AS total_inserts,
CASE WHEN a_delete.total_deletes IS NULL THEN 0 ELSE a_delete.total_deletes END AS total_deletes,
ur.last_activity_type,
CASE WHEN a_upload_count.upload_count IS NULL THEN 0 ELSE a_upload_count.upload_count END AS upload_count
FROM (SELECT id, last_activity_type FROM user_report WHERE is_active=true) AS ur
LEFT JOIN (SELECT user_id, count(*) total_updates FROM activitylog WHERE type = 'UPDATE' AND (($TIME - timestamp) < 172800) GROUP BY user_id) AS a_update ON ur.id = a_update.user_id
LEFT JOIN (SELECT user_id, count(*) total_inserts FROM activitylog WHERE type = 'INSERT' AND (($TIME - timestamp) < 172800) GROUP BY user_id) AS a_insert ON ur.id = a_insert.user_id
LEFT JOIN (SELECT user_id, count(*) total_deletes FROM activitylog WHERE type = 'DELETE' AND (($TIME - timestamp) < 172800) GROUP BY user_id) AS a_delete ON ur.id = a_delete.user_id
LEFT JOIN (SELECT user_id, count(*) AS upload_count FROM user_uploads WHERE (($TIME - timestamp) < 172800) GROUP BY user_id) AS a_upload_count ON ur.id = a_upload_count.user_id
ORDER BY upload_count DESC;"
if [ $? -eq 0 ];
then echo "made new active_user_report"
else echo "could not make new active_user_report"
exit
fi


# user_totals commands
hive -e "USE ${DATABASEN}; 
CREATE TABLE IF NOT EXISTS user_totals (time_ran BIGINT, total_users INT, users_added INT); 
INSERT INTO TABLE user_totals 
select $TIME, 
count(distinct u.id), 
CASE WHEN count(distinct ut.total_users)=0 THEN count(distinct u.id) ELSE count(distinct u.id) - max(struct(ut.time_ran, ut.total_users)).col2 END as users_added
FROM user as u FULL JOIN user_totals as ut
ORDER BY time_ran ASC;"
if [ $? -eq 0 ];
then echo "made new user_totals"
else echo "could not make new user_totals"
exit
fi
