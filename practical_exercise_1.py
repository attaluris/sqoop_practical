from airflow.operators import BashOperator
from airflow.models import DAG
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(minutes=1),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('practical_exercise_1', default_args=default_args, schedule_interval="*/45 * * * *", start_date=datetime.now() - timedelta(minutes=1))

load_data = BashOperator(
    task_id='load_data',
    bash_command="""python3 /home/cloudera/Downloads/practical/practical_exercise_data_generator.py --load_data """,
    dag=dag)

import_sql_hive = BashOperator(
    task_id='import_sql_hive',
    bash_command="""sh /home/cloudera/Downloads/practical/import_sql_hive.sh -u root -p /user/cloudera/password.txt -d practical_exercise_1 """,
    dag=dag)
    
create_csv = BashOperator(
    task_id='create_csv',
    bash_command="""python3 /home/cloudera/Downloads/practical/practical_exercise_data_generator.py --create_csv """,
    dag=dag)

import_csv_hive = BashOperator(
    task_id='import_csv_hive',
    bash_command="""sh /home/cloudera/Downloads/practical/import_csv_hive.sh -d practical_exercise_1 """,
    dag=dag)

generate_report = BashOperator(
    task_id='generate_report',
    bash_command="""sh /home/cloudera/Downloads/practical/generate_report.sh -d practical_exercise_1 """,
    dag=dag)

create_csv.set_downstream(import_csv_hive)
load_data.set_downstream(import_sql_hive)
import_sql_hive.set_downstream(generate_report)
import_csv_hive.set_downstream(generate_report)

