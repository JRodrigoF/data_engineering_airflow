import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

DAGS_FOLDER = '/opt/airflow/dags/'

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2020, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline_1 = DAG(
    dag_id='pipeline_1',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=DAGS_FOLDER,
)

source = DummyOperator(
    task_id='source',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

#TODO
deduplicate = BashOperator(
    task_id='deduplicate',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/remove_duplicates_kym.py --file /opt/airflow/dags/data/kym.json.gz --out /opt/airflow/dags/data/kym_unique.json",
    # bash_command="python {{ DAGS_FOLDER }}scripts/remove_duplicates_kym.py --file data/kym.json --out data/kym_unique.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

filter_1 = BashOperator(
    task_id='filter_1',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/prepare_to_tsv_kym.py --file /opt/airflow/dags/data/kym_unique.json --out /opt/airflow/dags/data/kym_unique_filter_1.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

core_tables = BashOperator(
    task_id='make_core_tables',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/data_to_tables.py --file /opt/airflow/dags/data/kym_unique_filter_1.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

info_dim = BashOperator(
    task_id='info_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_info.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_info.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

date_dim = BashOperator(
    task_id='date_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_dates.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_dates.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

parents_dim = BashOperator(
    task_id='parents_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_parents.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_parents.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

status_dim = BashOperator(
    task_id='status_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_status.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_status.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

origin_dim = BashOperator(
    task_id='origin_dim_tsv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/dim_origin.py --file /opt/airflow/dags/data/memes.tsv --out /opt/airflow/dags/data/dim_origins.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

end = DummyOperator(
    task_id='end',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

source >> deduplicate >> filter_1 >> core_tables >> [info_dim, date_dim, parents_dim, status_dim, origin_dim] >> end
