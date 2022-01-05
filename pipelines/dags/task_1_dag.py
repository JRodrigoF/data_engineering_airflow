import datetime
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/task_1/'

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline = DAG(
    dag_id='task_1',
    default_args=default_args_dict,
    catchup=False,
)

source = DummyOperator(
    task_id='source',
    dag=pipeline,
    trigger_rule='none_failed'
)

create_dag_folder = BashOperator(
    task_id='create_dag_folder',
    dag=pipeline,
    bash_command=("mkdir -p  {OUTPUT_FOLDER}").format(OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='none_failed'
)

KYM_data = DummyOperator(
    task_id='KYM_data',
    dag=pipeline,
    trigger_rule='none_failed'
)

remove_duplicates = BashOperator(
    task_id='remove_duplicates',
    dag=pipeline,
    bash_command=("mkdir -p {OUTPUT_FOLDER}; python "
                + " {SCRIPTS_FOLDER}remove_duplicates_kym.py "
                + "--file {DATA_FOLDER}kym.json.gz "
                + "--out {OUTPUT_FOLDER}{epoch}_kym_unique.json")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            DATA_FOLDER=DATA_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

filtering = BashOperator(
    task_id='filtering',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}prepare_to_tsv_kym.py "
                + "--file {OUTPUT_FOLDER}{epoch}_kym_unique.json "
                + "--out {OUTPUT_FOLDER}{epoch}_kym_unique_filter_1.json")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

KYM_data_to_tsv = BashOperator(
    task_id='KYM_data_to_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}data_to_tables.py "
                + "--file {OUTPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
                + "--outfolder {OUTPUT_FOLDER} "
                + "--prefix {epoch} ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

files = ['kym_unique.json'
    , 'kym_unique_filter_1.json'
    , 'meme_content_examples.tsv'
    , 'meme_details_types.tsv'
    , 'meme_tags.tsv'
    , 'memes.tsv'
    , 'parent_children_relations.tsv'
    , 'siblings_relations.tsv']
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

source >> create_dag_folder >> KYM_data >> remove_duplicates >> filtering >> KYM_data_to_tsv >> rename_output_files >> sink
