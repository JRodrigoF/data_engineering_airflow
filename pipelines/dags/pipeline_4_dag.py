import datetime
import pandas as pd
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.providers.neo4j.operators.neo4j import Neo4jOperator


DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipeline_4/'
NEO4J_INPUT_FOLDER =  'pipeline_4/'
POSTGRES_CONN_ID = "postgres_default"

NEO4J_CONN_ID = "neo4j_default"


def epoch_int(v):
    return int(v.strftime('%s'))

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

pipeline = DAG(
    dag_id='pipeline_4',
    default_args=default_args_dict,
    catchup=False,
    user_defined_filters={
        "epoch_int": epoch_int,
  }
)

source = DummyOperator(
    task_id='source',
    dag=pipeline,
    trigger_rule='none_failed'
)

KYM_data = DummyOperator(
    task_id='KYM_data',
    dag=pipeline,
    trigger_rule='none_failed'
)

Google_Vision_data = DummyOperator(
    task_id='Google_Vision_data',
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

memes_dim_tsv = BashOperator(
    task_id='memes_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}make_memes_dim_tsv.py "
                + "--file {OUTPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
                + "--outfile {OUTPUT_FOLDER}{epoch}_memes_dim.tsv ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

status_dim_tsv = BashOperator(
    task_id='status_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}dim_status.py "
                + "--file {OUTPUT_FOLDER}{epoch}_memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_status.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

date_dim_tsv = BashOperator(
    task_id='date_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_dates.py "
                + "--file {OUTPUT_FOLDER}{epoch}_memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_dates.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

origin_dim_tsv = BashOperator(
    task_id='origin_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_origin.py "
                + "--file {OUTPUT_FOLDER}{epoch}_memes.tsv "
                + "--out {OUTPUT_FOLDER}{epoch}_dim_origins.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

GV_data_to_tsv = BashOperator(
    task_id='GV_data_to_tsv',
    dag=pipeline,
    bash_command=("python "
    + "{SCRIPTS_FOLDER}GV_data_to_table.py "
    + "--file {DATA_FOLDER}kym_vision.json.gz "
    + "--out {OUTPUT_FOLDER}{epoch}_meme_safeness_relations.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER,
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

KYM_fact_table_tsv = BashOperator(
    task_id='KYM_fact_table_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}fact_table_maker_augmented.py "
                + "--folder {OUTPUT_FOLDER} "
                + "--out {OUTPUT_FOLDER}{epoch}_fact_table_memes.tsv "
                + "--prefix {epoch} ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER,
            epoch="{{ execution_date.int_timestamp }}"),
    trigger_rule='all_success',
    depends_on_past=False,
)

db_init = PostgresOperator(
    task_id='db_init',
    dag=pipeline,
    postgres_conn_id='postgres_default',
    sql=[
        'DROP DATABASE IF EXISTS memes',
        'CREATE DATABASE memes',
    ],
    trigger_rule='all_success',
    autocommit=True,
)

postgres_db = PostgresOperator(
    task_id='postgres_db',
    dag=pipeline,
    postgres_conn_id='postgres_default',
    # sql='{}create_tables.sql'.format(SCRIPTS_FOLDER), # does not find the file
    sql='sql/create_tables_augmented.sql',
    trigger_rule='all_success',
    autocommit=True,
)

def _postgres_populate_db(output_folder: str, postgres_conn_id: str, epoch: str):
    hook = PostgresHook.get_hook(postgres_conn_id)

    tables = ['dim_dates', 'dim_status', 'dim_origins', 'dim_safeness_gv', 'fact_table_memes']
    for table_name in tables:
        tmp_table = f'{output_folder}/temp_{epoch}_{table_name}.tsv'
        df_temp = pd.read_csv(
            f'{output_folder}{epoch}_{table_name}.tsv', sep="\t")
        df_temp.to_csv(tmp_table, sep="\t", encoding='utf-8', na_rep='None',
                        header=False, index=False)
        # hook.run(f"COPY {table_name} FROM '{tmp_table}.tsv' DELIMITER '\t' CSV HEADER")
        hook.bulk_load(table_name, tmp_table)
        os.remove(tmp_table)


populate_db = PythonOperator(
    task_id='populate_db',
    dag=pipeline,
    python_callable=_postgres_populate_db,
    op_kwargs={
        'output_folder': OUTPUT_FOLDER,
        'postgres_conn_id': POSTGRES_CONN_ID,
        'epoch': "{{ execution_date.int_timestamp }}"
    },
    trigger_rule='all_success',
)



make_meme_similarity_facts_csv = BashOperator(
    task_id='make_meme_similarity_facts_csv',
    dag=pipeline,
    bash_command= ("python "
        + " {SCRIPTS_FOLDER}memes_similarity_facts.py "
        + " --file {INPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
        + " --threshold=0.5 "
        + " --out {OUTPUT_FOLDER}memes_similarity_score.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, INPUT_FOLDER=OUTPUT_FOLDER, epoch="{{ execution_date.int_timestamp }}", OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)


extract_lda_topics = BashOperator(
    task_id='extract_lda_topics',
    dag=pipeline,
    bash_command = ("python "
        + " {SCRIPTS_FOLDER}lda_topics.py "
        + " --file {INPUT_FOLDER}{epoch}_kym_unique_filter_1.json "
        + " --outtopics {OUTPUT_FOLDER}lda_topics.tsv "
        + " --outmemes {OUTPUT_FOLDER}memes_lda_topics.tsv "
        + " --outkeywords {OUTPUT_FOLDER}lda_topic_keywords.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, INPUT_FOLDER=OUTPUT_FOLDER, epoch="{{ execution_date.int_timestamp }}", OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

neo4j_start_creating_nodes = DummyOperator(
    task_id='neo4j_start_creating_nodes',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

neo4j_delete_all_from_db = Neo4jOperator(
    task_id='neo4j_delete_all_from_db',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=" MATCH (n) DETACH DELETE n; \n ",
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

neo4j_start_creating_indexes = DummyOperator(
    task_id='neo4j_start_creating_indexes',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)




TIMESTAMP = '1641051821'


neo4j_create_meme_nodes = Neo4jOperator(
    task_id='neo4j_create_Meme_nodes',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql= (" LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}{epoch}_memes.tsv\" AS row FIELDTERMINATOR '\t' \n"
        + " MERGE (m:Meme {{Id: row.Id, category:row.category, status:row.details_status, description:row.meta_description}}) "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER, epoch=TIMESTAMP),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)



neo4j_create_ldatopic_nodes = Neo4jOperator(
    task_id='neo4j_create_LDATopic_nodes',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql= (" LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}lda_topics.tsv\" AS row FIELDTERMINATOR '\t' \n"
        + " MERGE (m:LDATopic {{Id: row.Id, name:row.name}}) "
        ).format(INPUT_FOLDER=NEO4J_INPUT_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

neo4j_start_creating_relations = DummyOperator(
    task_id='neo4j_start_creating_relations',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

neo4j_start_creating_nodes >>[neo4j_create_meme_nodes, neo4j_create_ldatopic_nodes] >> neo4j_start_creating_relations


neo4j_create_meme_child_relations = Neo4jOperator(
    task_id='neo4j_create_meme_child_relations',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}{epoch}_parent_children_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n"
        + " MERGE (m:Meme {{Id: row.meme_Id}}) \n"
        + " MERGE (n:Meme {{Id: row.child}}) \n "
        + " CREATE (m)-[r:HAS_CHILD]->(n)  \n "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER, epoch=TIMESTAMP),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

#TODO save only meme1_Id>meme2_Id
neo4j_create_meme_sibling_relations = Neo4jOperator(
    task_id='neo4j_create_meme_sibling_relations',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}{epoch}_siblings_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:Meme {{Id: row.meme_Id}})  \n "
        + " MERGE (n:Meme {{Id: row.sibling}})  \n "
        + " CREATE (m)-[r:HAS_SIBLING]->(n)  \n "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER, epoch=TIMESTAMP),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

#nodes of text - keywords and tags
neo4j_create_meme_Text_has_tag_rel = Neo4jOperator(
    task_id='neo4j_create_meme_Text_has_tag_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}{epoch}_meme_tags.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:Meme {{Id: row.key_KYM}})  \n "
        + " MERGE (n:Text {{Id: row.tag}})  \n "
        + " CREATE (m)-[r:HAS_TAG]->(n)  \n "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER, epoch=TIMESTAMP),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

neo4j_create_LDAtopic_Text_has_keyword_rel = Neo4jOperator(
    task_id='neo4j_create_LDAtopic_Text_has_keyword_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}lda_topic_keywords.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:LDATopic {{Id: row.topic_Id}})  \n "
        + " MERGE (n:Text {{Id: row.keyword}})  \n "
        + " CREATE (m)-[r:HAS_KEYWORD{{score:toFloat(row.keyword_prob_scores)}}]->(n)  \n "
        ).format(INPUT_FOLDER=NEO4J_INPUT_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

neo4j_create_Meme_has_similar_description_rel = Neo4jOperator(
    task_id='neo4j_create_Meme_has_similar_description_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 100000 \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
        + " WHERE toFloat(row.desc_similarity) >= 0.2 \n "
        + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
        + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
        + " CREATE (m)-[r:HAS_SIMILAR_DESC{{score: toFloat(row.desc_similarity)}}]->(n) \n "
        ).format(INPUT_FOLDER=NEO4J_INPUT_FOLDER),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )

neo4j_create_Meme_has_similar_tags_rel = Neo4jOperator(
    task_id='neo4j_create_Meme_has_similar_tags_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 100000 \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
        + " WHERE toFloat(row.tags_similarity) >= 0.2 \n "
        + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
        + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
        + " CREATE (m)-[r:HAS_SIMILAR_TAGS{{score: toFloat(row.tags_similarity)}}]->(n) \n "
        ).format(INPUT_FOLDER=NEO4J_INPUT_FOLDER),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )



sink = DummyOperator(
    task_id='sink',
    dag=pipeline,
    trigger_rule='none_failed'
)





source >> KYM_data >> remove_duplicates >> filtering >> KYM_data_to_tsv >> \
    [date_dim_tsv, status_dim_tsv, origin_dim_tsv, memes_dim_tsv] >> \
    KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> sink

KYM_data_to_tsv >> [extract_lda_topics, make_meme_similarity_facts_csv] >> neo4j_start_creating_indexes >> neo4j_delete_all_from_db

for node in ['LDATopic', 'Meme', 'Text']:
    neo4j_create_index_node = Neo4jOperator(
        task_id="neo4j_create_index_"+node+"_node",
        neo4j_conn_id=NEO4J_CONN_ID,
        sql= ("CREATE BTREE INDEX {node}_Id IF NOT EXISTS FOR (n:{node}) ON (n.Id); "
            ).format(node=node),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )
    neo4j_delete_all_from_db >> neo4j_create_index_node
    neo4j_create_index_node >> neo4j_start_creating_nodes

neo4j_start_creating_relations >> [neo4j_create_Meme_has_similar_description_rel, neo4j_create_Meme_has_similar_tags_rel, neo4j_create_meme_Text_has_tag_rel, neo4j_create_LDAtopic_Text_has_keyword_rel, neo4j_create_meme_child_relations, neo4j_create_meme_sibling_relations] >> sink

source >> Google_Vision_data >> GV_data_to_tsv >> safeness_dim_tsv >> \
    KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> sink
