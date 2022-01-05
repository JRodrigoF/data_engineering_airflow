import datetime
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'

OUTPUT_FOLDER = DAGS_FOLDER + 'output/task_2/'

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


pipeline = DAG(
    dag_id='task_2',
    default_args=default_args_dict,
    catchup=False,
)

create_dag_folder = BashOperator(
    task_id='create_dag_folder',
    dag=pipeline,
    bash_command=("mkdir -p  {OUTPUT_FOLDER}").format(OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='none_failed'
)

Google_Vision_data = DummyOperator(
    task_id='Google_Vision_data',
    dag=pipeline,
    trigger_rule='none_failed'
)




GV_data_to_tsv = BashOperator(
    task_id='GV_data_to_tsv',
    dag=pipeline,
    bash_command=("python "
    + "{SCRIPTS_FOLDER}GV_data_to_table.py "
    + "--file {DATA_FOLDER}kym_vision.json.gz "
    + "--out {OUTPUT_FOLDER}{epoch}_meme_safeness_relations.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            DATA_FOLDER=DATA_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

safeness_dim_tsv = BashOperator(
    task_id='safeness_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_safeness_gv.py "
                + "--file {OUTPUT_FOLDER}{epoch}_meme_safeness_relations.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_safeness_gv.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

files = ['dim_safeness_gv.tsv'
    , 'meme_safeness_relations.tsv'
    ]
commands = []
for f in files:
    commands.append('cp {OUTPUT_FOLDER}{epoch}_%s {OUTPUT_FOLDER}%s '% (f, f))
command = " && ".join(commands)
rename_output_files = BashOperator(
    task_id='rename_output_files',
    dag=pipeline,
    bash_command=(command).format(
            OUTPUT_FOLDER=OUTPUT_FOLDER
            , epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False
)

sink = DummyOperator(
    task_id='sink',
    dag=pipeline,
    trigger_rule='none_failed'
)

create_dag_folder >> Google_Vision_data >> GV_data_to_tsv >> safeness_dim_tsv >> rename_output_files >> sink
