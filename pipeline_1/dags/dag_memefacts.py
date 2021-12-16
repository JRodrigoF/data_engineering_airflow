import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook


import pandas as pd
import os
POSTGRES_CONN_ID = "postgres_default"

DAGS_FOLDER = '/opt/airflow/dags/'

SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
TEMPLATE_FOLDER = DAGS_FOLDER + 'data/'


def _check_cleanup_is_needed(ti, epoch, data_folder):
    if (os.path.isfile(f"{data_folder}/{epoch}_kym_unique_filter_1")):
        return 'cleanup_files'
    else:
        return 'end'


def _postgres_import_memes_similarity_facts(epoch: int, data_folder: str):
    global POSTGRES_CONN_ID
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID).get_conn()
    #conn = PostgresHook(postgres_conn_id=db).get_conn()
    cur = pg_hook.cursor()

    cur.execute("CREATE TABLE IF NOT EXISTS memes_similarity_facts (\n"
        "meme_id1 VARCHAR,\n"
        "meme_id2 VARCHAR,\n"
        "common_tags INT,\n"
        "tags_similarity NUMERIC(3,2),\n"
        "desc_similarity NUMERIC(3,2)\n"
        ");\n")
    cur.execute(" TRUNCATE memes_similarity_facts ")
    SQL_STATEMENT = """
        COPY memes_similarity_facts FROM STDIN WITH DELIMITER AS E'\t'
        """
    with open(data_folder + f'{epoch}_memes_similarity_score.tsv', 'r') as f:
        #to skip header
        first_line = f.readline()
        cur.copy_expert(SQL_STATEMENT, f)
        pg_hook.commit()

def _postgres_import_memes_dim(epoch: int, data_folder: str):
    global POSTGRES_CONN_ID
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID).get_conn()
    #conn = PostgresHook(postgres_conn_id=db).get_conn()
    cur = pg_hook.cursor()

    cur.execute(" DROP TABLE IF EXISTS memes_dim ")
    cur.execute("CREATE TABLE IF NOT EXISTS memes_dim (\n"
        "--meme_id SERIAL PRIMARY KEY,\n"
        "meme_kym_id VARCHAR,\n"
        "name VARCHAR,\n"
        "added INT,\n"
        "description VARCHAR,\n"
        "origin VARCHAR,\n"
        "status VARCHAR,\n"
        "year INT,\n"
        "last_update INT,\n"
        "image_url VARCHAR,\n"
        "url VARCHAR,\n"
        "parent_kym_id VARCHAR,\n"
        "tags JSONB"
        ");\n")
    #cur.execute(" TRUNCATE memes_dim ")
    #columns = ['kym_id', 'name', 'added', 'origin', 'tags', 'status', 'year', 'last_update', 'description', 'image_url', 'url', 'parent_kym_id'  ]

    SQL_STATEMENT = """
            COPY memes_dim(meme_kym_id, name, added, origin, status, year,last_update, description, image_url, url, parent_kym_id, tags )
            FROM STDIN  WITH CSV NULL AS 'null' DELIMITER AS E'\t'"""

    with open(data_folder + f'{epoch}_memes_dim.tsv', 'r') as f:
        #to skip header
        first_line = f.readline()
        cur.copy_expert(SQL_STATEMENT, f)
        pg_hook.commit()

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2020, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline_memesimilarity = DAG(
    dag_id='pipeline_memesimilarity',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath = TEMPLATE_FOLDER,
)

#TODO
remove_duplicates = BashOperator(
    task_id='remove_duplicates',
    dag=pipeline_memesimilarity,
    bash_command=("python "
        + " {SCRIPTS_FOLDER}remove_duplicates_kym.py "
        + "--file {DATA_FOLDER}kym.json.gz "
        + "--out {DATA_FOLDER}{epoch}_kym_unique.json"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

filtering_step_1 = BashOperator(
    task_id='filtering_step_1',
    dag=pipeline_memesimilarity,
    bash_command=("python "
        + " {SCRIPTS_FOLDER}prepare_to_tsv_kym.py "
        + "--file {DATA_FOLDER}{epoch}_kym_unique.json "
        + "--out {DATA_FOLDER}{epoch}_kym_unique_filter_1.json"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

meme_data_to_csv = BashOperator(
    task_id='meme_data_to_csv',
    dag=pipeline_memesimilarity,
    bash_command=("python "
        + " {SCRIPTS_FOLDER}data_to_tables.py "
        + "--file {DATA_FOLDER}{epoch}_kym_unique_filter_1.json "
        + "--outfolder {DATA_FOLDER} "
        + "--prefix {epoch} "
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

make_memes_dim_csv = BashOperator(
    task_id='make_memes_dim_csv',
    dag=pipeline_memesimilarity,
    bash_command=("python "
        + " {SCRIPTS_FOLDER}make_memes_dim_csv.py "
        + "--file {DATA_FOLDER}{epoch}_kym_unique_filter_1.json "
        + "--outfile {DATA_FOLDER}{epoch}_memes_dim.tsv "
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)


make_meme_similarity_facts_csv = BashOperator(
    task_id='make_meme_similarity_facts_csv',
    dag=pipeline_memesimilarity,
    bash_command= ("python "
        + " {SCRIPTS_FOLDER}memes_similarity_facts.py "
        + " --file {DATA_FOLDER}{epoch}_kym_unique_filter_1.json "
        + " --threshold=0.2 "
        + " --out {DATA_FOLDER}{epoch}_memes_similarity_score.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)


extract_lda_topics = BashOperator(
    task_id='extract_lda_topics',
    dag=pipeline_memesimilarity,
    bash_command = ("python "
        + " {SCRIPTS_FOLDER}extract_lda_topics.py "
        + " --file {DATA_FOLDER}{epoch}_kym_unique_filter_1.json "
        + " --outtopics {DATA_FOLDER}{epoch}_lda_topics.tsv "
        + " --outmemes {DATA_FOLDER}{epoch}_memes_lda_topics.tsv "
        + " --outkeywords {DATA_FOLDER}{epoch}_lda_topic_keywords.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)


start_inserting_dims = DummyOperator(
    task_id='start_inserting_dims',
    dag=pipeline_memesimilarity,
    trigger_rule='all_success',
)

start_inserting_facts = DummyOperator(
    task_id='start_inserting_facts',
    dag=pipeline_memesimilarity,
    trigger_rule='all_success',
)


import_memes_similarity_facts = PythonOperator(
    task_id='import_memes_similarity_facts',
    dag=pipeline_memesimilarity,
    python_callable=_postgres_import_memes_similarity_facts,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'data_folder': DATA_FOLDER,
    },
    trigger_rule='all_success',
)


import_memes_dim = PythonOperator(
    task_id='import_memes_dim',
    dag=pipeline_memesimilarity,
    python_callable=_postgres_import_memes_dim,
    op_kwargs={
        'epoch': '{{ execution_date.int_timestamp }}',
        'data_folder': DATA_FOLDER,
    },
    trigger_rule='all_success',
)


check_if_cleanup_is_needed = BranchPythonOperator(
    task_id='check_if_cleanup_is_needed',
    dag=pipeline_memesimilarity,
    python_callable=_check_cleanup_is_needed,
    op_kwargs = {
        'data_folder' : DATA_FOLDER,
        'epoch' : "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)

cleanup_files = BashOperator(
    task_id='cleanup_files',
    dag=pipeline_memesimilarity,
    bash_command=("rm  "
        + " {DATA_FOLDER}{epoch}_*.tsv "
        ).format(DATA_FOLDER=DATA_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)


end = DummyOperator(
    task_id='end',
    dag=pipeline_memesimilarity,
    trigger_rule='none_failed'
)



remove_duplicates >> filtering_step_1 >> [meme_data_to_csv, make_meme_similarity_facts_csv, extract_lda_topics, make_memes_dim_csv]>> start_inserting_dims
start_inserting_dims >> import_memes_dim >> start_inserting_facts
start_inserting_facts >> import_memes_similarity_facts >> check_if_cleanup_is_needed
check_if_cleanup_is_needed >> [cleanup_files, end]
cleanup_files >> end
