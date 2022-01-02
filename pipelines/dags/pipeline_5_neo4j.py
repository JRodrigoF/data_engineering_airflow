import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.neo4j.operators.neo4j import Neo4jOperator

DAGS_FOLDER = '/opt/airflow/dags/'

INPUT_FOLDER1 =  'pipeline_2/'
INPUT_FOLDER2 =  'pipeline_3/'

SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
TEMPLATE_FOLDER = DAGS_FOLDER + 'data/'

TIMESTAMP = '1640976918'


import pandas as pd
import os

NEO4J_CONN_ID = "neo4j_default"

DAGS_FOLDER = '/opt/airflow/dags/'

SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
TEMPLATE_FOLDER = DAGS_FOLDER + 'data/'

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 1, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

DAG_ID = 'pipeline_5_neo4j_dag'


pipeline = DAG(
    dag_id=DAG_ID,
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=DAGS_FOLDER,
)

start_creating_nodes = DummyOperator(
    task_id='start_creating_nodes',
    dag=pipeline
)

neo4j_delete_all_from_db = Neo4jOperator(
    task_id='neo4j_delete_all_from_db',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=" MATCH (n) DETACH DELETE n; \n ",
    dag=pipeline,
)

start_creating_indexes = DummyOperator(
    task_id='start_creating_indexes',
    dag=pipeline
)

start_creating_indexes >> neo4j_delete_all_from_db
for node in ['LDATopic', 'Meme', 'Text']:
    neo4j_create_index_node = Neo4jOperator(
        task_id="neo4j_create_index_"+node+"_node",
        neo4j_conn_id=NEO4J_CONN_ID,
        sql= ("CREATE BTREE INDEX {node}_Id IF NOT EXISTS FOR (n:{node}) ON (n.Id); "
            ).format(node=node),
        dag=pipeline,
    )
    neo4j_delete_all_from_db >> neo4j_create_index_node
    neo4j_create_index_node >> start_creating_nodes


neo4j_create_meme_nodes = Neo4jOperator(
    task_id='neo4j_create_Meme_nodes',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql= (" LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}{epoch}_memes.tsv\" AS row FIELDTERMINATOR '\t' \n"
        + " MERGE (m:Meme {{Id: row.Id, category:row.category, status:row.details_status}}) "
        ).format(INPUT_FOLDER1=INPUT_FOLDER1, epoch=TIMESTAMP),
    dag=pipeline,
)



neo4j_create_ldatopic_nodes = Neo4jOperator(
    task_id='neo4j_create_LDATopic_nodes',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql= (" LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}lda_topics.tsv\" AS row FIELDTERMINATOR '\t' \n"
        + " MERGE (m:LDATopic {{Id: row.Id, name:row.name}}) "
        ).format(INPUT_FOLDER=INPUT_FOLDER2),
    dag=pipeline,
)

start_creating_relations = DummyOperator(
    task_id='start_creating_relations',
    dag=pipeline,
)

start_creating_nodes >>[neo4j_create_meme_nodes, neo4j_create_ldatopic_nodes] >> start_creating_relations

neo4j_create_meme_child_relations = Neo4jOperator(
    task_id='neo4j_create_meme_child_relations',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}{epoch}_parent_children_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n"
        + " MERGE (m:Meme {{Id: row.meme_Id}}) \n"
        + " MERGE (n:Meme {{Id: row.child}}) \n "
        + " CREATE (m)-[r:HAS_CHILD]->(n)  \n "
        ).format(INPUT_FOLDER1=INPUT_FOLDER1, epoch=TIMESTAMP),
    dag=pipeline,
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
        ).format(INPUT_FOLDER1=INPUT_FOLDER1, epoch=TIMESTAMP),
    dag=pipeline,
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
        ).format(INPUT_FOLDER1=INPUT_FOLDER1, epoch=TIMESTAMP),
    dag=pipeline,
)

neo4j_create_LDAtopic_Text_has_keyword_rel = Neo4jOperator(
    task_id='neo4j_create_LDAtopic_Text_has_keyword_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}lda_topic_keywords.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:LDATopic {{Id: row.topic_Id}})  \n "
        + " MERGE (n:Text {{Id: row.keyword}})  \n "
        + " CREATE (m)-[r:HAS_KEYWORD{{score:toFloat(row.keyword_prob_scores)}}]->(n)  \n "
        ).format(INPUT_FOLDER=INPUT_FOLDER2),
    dag=pipeline,
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
        ).format(INPUT_FOLDER=INPUT_FOLDER2),
            dag=pipeline,
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
        ).format(INPUT_FOLDER=INPUT_FOLDER2),
            dag=pipeline,
    )


end = DummyOperator(
    task_id='end',
    dag=pipeline
)



start_creating_relations >> [neo4j_create_Meme_has_similar_description_rel, neo4j_create_Meme_has_similar_tags_rel, neo4j_create_meme_Text_has_tag_rel, neo4j_create_LDAtopic_Text_has_keyword_rel, neo4j_create_meme_child_relations, neo4j_create_meme_sibling_relations] >> end
