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

#TODO
task_one = BashOperator(
    task_id='remove_duplicates',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/remove_duplicates_kym.py --file /opt/airflow/dags/data/kym.json.gz --out /opt/airflow/dags/data/kym_unique.json",
    # bash_command="python {{ DAGS_FOLDER }}scripts/remove_duplicates_kym.py --file data/kym.json --out data/kym_unique.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

task_two = BashOperator(
    task_id='filtering_step_1',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/prepare_to_tsv_kym.py --file /opt/airflow/dags/data/kym_unique.json --out /opt/airflow/dags/data/kym_unique_filter_1.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

task_three_a = BashOperator(
    task_id='meme_data_to_csv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/data_to_tables.py --file /opt/airflow/dags/data/kym_unique_filter_1.json",
    trigger_rule='all_success',
    depends_on_past=False,
)

task_three_b = BashOperator(
    task_id='make_tags_cosine_similarity_csv',
    dag=pipeline_1,
    bash_command= "python /opt/airflow/dags/scripts/calculate_cos_similarity.py --file /opt/airflow/dags/data/kym_unique_filter_1.json --type tag --threshold=0.1 --out /opt/airflow/dags/data/memes_tags_similarity_score.tsv ",
    trigger_rule='all_success',
    depends_on_past=False,
)

task_three_c = BashOperator(
    task_id='make_desc_cosine_similarity_csv',
    dag=pipeline_1,
    bash_command="python /opt/airflow/dags/scripts/calculate_cos_similarity.py --file /opt/airflow/dags/data/kym_unique_filter_1.json --type desc --threshold=0.1 --out /opt/airflow/dags/data/memes_desc_similarity_score.tsv ",
    trigger_rule='all_success',
    depends_on_past=False,
)

task_three_d = BashOperator(
    task_id='extract_lda_topics',
    dag=pipeline_1,
    bash_command = "python /opt/airflow/dags/scripts/extract_lda_topics.py --file /opt/airflow/dags/data/kym_unique_filter_1.json --outtopics /opt/airflow/dags/data/lda_topics.tsv --outmemes /opt/airflow/dags/data/memes_lda_topics.tsv --outkeywords /opt/airflow/dags/data/lda_topic_keywords.tsv",
    trigger_rule='all_success',
    depends_on_past=False,
)

end = DummyOperator(
    task_id='end',
    dag=pipeline_1,
    trigger_rule='none_failed'
)

#task_one >> task_two >> task_three >> end
task_one >> task_two >> [task_three_a, task_three_b, task_three_c, task_three_d]>> end
