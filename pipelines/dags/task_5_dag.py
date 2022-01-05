import datetime
import os
import pandas as pd
from airflow import DAG

from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import  PythonOperator

from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook


DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'

#without prefix, because neo4j import folder is already mounted to DAGS_FOLDER + 'output/ folder
TASK1_FOLDER = 'task_1/'
TASK4_FOLDER = 'task_4/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/task_5/'


NEO4J_CONN_ID = "neo4j_default"

default_args_dict = {
    # cron sintax: * * * * *
    'start_date': datetime.datetime(2021, 12, 1, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",   # run now
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}


pipeline = DAG(
    dag_id='task_5',
    default_args=default_args_dict,
    catchup=False,
)

create_dag_folder = BashOperator(
    task_id='create_dag_folder',
    dag=pipeline,
    bash_command=("mkdir -p  {OUTPUT_FOLDER}").format(OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='none_failed'
)

prepare_neo4j_db = Neo4jOperator(
    task_id='prepare_neo4j_db',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=" MATCH (n) DETACH DELETE n; \n ",
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

create_indexes = DummyOperator(
    task_id='create_indexes',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)


create_nodes = DummyOperator(
    task_id='create_nodes',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)


create_dag_folder >> prepare_neo4j_db >> create_indexes

for node in ['LDATopic', 'Meme', 'Text']:
    create_index = Neo4jOperator(
        task_id= node+"_index",
        neo4j_conn_id=NEO4J_CONN_ID,
        sql= (
            "CREATE BTREE INDEX {node}_Id IF NOT EXISTS FOR (n:{node}) ON (n.Id); "
            ).format(node=node),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )
    create_indexes >> create_index
    create_index >> create_nodes



create_Memes = Neo4jOperator(
    task_id='create_Memes',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql= (" USING PERIODIC COMMIT 1000  \n "
          +  " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER}memes.tsv\"  AS row FIELDTERMINATOR '\t' WITH row \n "
          + " MERGE (m:Meme {{Id: row.Id, category:row.category, status:row.details_status, description:row.meta_description, url:row.url}}) "
        ).format(TASK1_FOLDER=TASK1_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

create_LDAtopics = Neo4jOperator(
    task_id='create_LDAtopics',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql= (" LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER}lda_topics.tsv\" AS row FIELDTERMINATOR '\t' \n"
        + " MERGE (m:LDATopic {{Id: row.Id, name:row.name}}) "
        ).format(TASK4_FOLDER=TASK4_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

create_relations1 = DummyOperator(
    task_id='create_relations1',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

create_relations2 = DummyOperator(
    task_id='create_relations2',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

create_nodes >> [create_Memes, create_LDAtopics] >> create_relations1



Meme_childs = Neo4jOperator(
    task_id='Meme_childs',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER}parent_children_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n"
        + " MERGE (m:Meme {{Id: row.meme_Id}}) \n"
        + " MERGE (n:Meme {{Id: row.child}}) \n "
        + " CREATE (m)-[r:HAS_CHILD]->(n)  \n "
        ).format(TASK1_FOLDER=TASK1_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

#TODO save only meme1_Id>meme2_Id
Meme_siblings = Neo4jOperator(
    task_id='Meme_siblings',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER}siblings_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:Meme {{Id: row.meme_Id}})  \n "
        + " MERGE (n:Meme {{Id: row.sibling}})  \n "
        + " CREATE (m)-[r:HAS_SIBLING]->(n)  \n "
        ).format(TASK1_FOLDER=TASK1_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

#nodes of text - keywords and tags
Meme_Tags = Neo4jOperator(
    task_id='Meme_Tags',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER}meme_tags.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:Meme {{Id: row.key_KYM}})  \n "
        + " MERGE (n:Text {{Id: row.tag}})  \n "
        + " CREATE (m)-[r:HAS_TAG]->(n)  \n "
        ).format(TASK1_FOLDER=TASK1_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

LDAtopic_Keywords = Neo4jOperator(
    task_id='LDAtopic_Keywords',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER}lda_topic_keywords.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:LDATopic {{Id: row.topic_Id}})  \n "
        + " MERGE (n:Text {{Id: row.keyword}})  \n "
        + " CREATE (m)-[r:HAS_KEYWORD{{score:toFloat(row.keyword_prob_scores)}}]->(n)  \n "
        ).format(TASK4_FOLDER=TASK4_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

Meme_LDAtopics = Neo4jOperator(
    task_id='Meme_LDAtopics',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER}memes_lda_topics.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (l:LDATopic {{Id: row.topic_Id}})  \n "
        + " MERGE (m:Meme {{Id: row.meme_Id}})  \n "
        + " CREATE (m)-[r:HAS_TOPIC{{score:toFloat(row.topic_prob_score)}}]->(l)  \n "
        ).format(TASK4_FOLDER=TASK4_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

Meme_has_similar_desc = Neo4jOperator(
    task_id='Meme_has_similar_desc',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 100000 \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
        + " WHERE toFloat(row.desc_similarity) >= 0.2 \n "
        + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
        + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
        + " CREATE (m)-[r:HAS_SIMILAR_DESC{{score: toFloat(row.desc_similarity)}}]->(n) \n "
        ).format(TASK4_FOLDER=TASK4_FOLDER),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )

Meme_has_common_tags = Neo4jOperator(
    task_id='Meme_has_common_tags',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 100000 \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
        + " WHERE toInteger(row.common_tags) >= 5 \n "
        + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
        + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
        + " CREATE (m)-[r:HAS_COMMON_TAGS{{nr: toInteger(row.common_tags)}}]->(n) \n "
        ).format(TASK4_FOLDER=TASK4_FOLDER),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )

Meme_has_similar_tags = Neo4jOperator(
    task_id='Meme_has_similar_tags',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 100000 \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
        + " WHERE toFloat(row.tags_similarity) >= 0.2 \n "
        + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
        + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
        + " CREATE (m)-[r:HAS_SIMILAR_TAGS{{score: toFloat(row.tags_similarity)}}]->(n) \n "
        ).format(TASK4_FOLDER=TASK4_FOLDER),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )

queries_to_tsv = DummyOperator(
    task_id='queries_to_tsv',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

create_relations1 >> [ Meme_childs, Meme_siblings \
    , Meme_Tags \
    , Meme_has_common_tags \
    , Meme_has_similar_desc ] >> create_relations2


create_relations2 >> [ LDAtopic_Keywords \
    , Meme_LDAtopics \
    , Meme_has_similar_tags ] >> queries_to_tsv



def _make_query(ti, output_folder, query, output_file):
    global NEO4J_CONN_ID
    neo4j_hook  = Neo4jHook(NEO4J_CONN_ID).get_conn()

    def write_to_tsv(tx, output_file):
        columns = []
        rows = []
        df = pd.DataFrame()
        for record in tx.run(query):
            if not len(columns):
                columns = record.keys()
            rows.append([record[c]  for c in columns]  )

        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(output_file, sep='\t', index = False)

    with neo4j_hook.session() as session:
        session.read_transaction(write_to_tsv, output_file)

    #neo4j_hook.close()


#MOST TAGS IN COMMON
query1 = PythonOperator(
    task_id='query1',
    dag=pipeline,
    python_callable=_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'query1_Top100MemesWithTagsInCommon.tsv',
        'query': """
            MATCH (m1:Meme)-[r:HAS_COMMON_TAGS]->(m2:Meme)
            RETURN r.nr AS tags_in_common, m1.Id AS meme1_Id, m2.Id AS meme2_Id
            ORDER BY r.nr DESC, m1.Id
            LIMIT 100
            """,
    },
    depends_on_past=False,
)


#VERY SIMILAR BUT HAVE NO COMMON TAGS
query2 = PythonOperator(
    task_id='query2',
    dag=pipeline,
    python_callable=_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'query2_VerySimilarMemesNoTagsInCommon.tsv',
        'query': """
            MATCH  (m)-[r1:HAS_SIMILAR_DESC]-(n)
            WHERE r1.score>0.5 and not((m)-[:HAS_TAG]->()<-[:HAS_TAG]-(n)) and id(n) > id(m)
            RETURN
                m.Id AS meme1_Id
                , n.Id AS meme2_Id
                , r1.score AS similarity_score
                , m.description AS meme1_description
                , n.description AS meme2_description
            ORDER BY r1.score DESC, m.Id, n.Id
            """,
    },
    depends_on_past=False,
)

#TOP 10 MEMES WITH MOST DESCENDANTS

query3 = PythonOperator(
    task_id='query3',
    dag=pipeline,
    python_callable=_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'query3_Top10MostDescendants.tsv',
        'query': """
            MATCH (p{category:'Meme'}) -[:HAS_CHILD *1..]-> (n)
            RETURN COUNT(DISTINCT n) as total_descendants, p.Id as meme_Id
            ORDER BY total_descendants DESC, meme_Id
            LIMIT 10
            """,
    },
    depends_on_past=False,
)

#TOP 10 LONGEST CHAINS
query4 = PythonOperator(
    task_id='query4',
    dag=pipeline,
    python_callable=_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'query4_Top10LongestParentChildChains.tsv',
        'query': """
            MATCH path = (:Meme{category:'Meme'})-[:HAS_CHILD*]->(next)
            WHERE not((next)-[:HAS_CHILD]->()) AND length(path)>4
            RETURN [t in nodes(path) | t.Id] AS chain
            ORDER BY length(path) DESC
            """,
    },
    depends_on_past=False,
)




sink = DummyOperator(
    task_id='sink',
    dag=pipeline,
    trigger_rule='none_failed'
)

queries_to_tsv >> [query1, query2, query3, query4]   >> sink
