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
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook


DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'
OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipeline_4/'
NEO4J_INPUT_FOLDER =  'pipeline_4/'
POSTGRES_CONN_ID = "postgres_default"

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
    dag_id='pipeline_4_neo4j',
    default_args=default_args_dict,
    catchup=False
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
                + "--out {OUTPUT_FOLDER}kym_unique.json")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            DATA_FOLDER=DATA_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

filtering = BashOperator(
    task_id='filtering',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}prepare_to_tsv_kym.py "
                + "--file {OUTPUT_FOLDER}kym_unique.json "
                + "--out {OUTPUT_FOLDER}kym_unique_filter_1.json")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False
)

KYM_data_to_tsv = BashOperator(
    task_id='KYM_data_to_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}data_to_tables.py "
                + "--file {OUTPUT_FOLDER}kym_unique_filter_1.json "
                + "--outfolder {OUTPUT_FOLDER} ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

memes_dim_tsv = BashOperator(
    task_id='memes_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}make_memes_dim_tsv.py "
                + "--file {OUTPUT_FOLDER}kym_unique_filter_1.json "
                + "--outfile {OUTPUT_FOLDER}memes_dim.tsv ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

status_dim_tsv = BashOperator(
    task_id='_status_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + " {SCRIPTS_FOLDER}dim_status.py "
                + "--file {OUTPUT_FOLDER}memes.tsv "
                + "--out {OUTPUT_FOLDER}dim_status.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

date_dim_tsv = BashOperator(
    task_id='date_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_dates.py "
                + "--file {OUTPUT_FOLDER}memes.tsv "
                + "--out {OUTPUT_FOLDER}dim_dates.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

origin_dim_tsv = BashOperator(
    task_id='origin_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_origin.py "
                + "--file {OUTPUT_FOLDER}memes.tsv "
                + "--out {OUTPUT_FOLDER}dim_origins.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

GV_data_to_tsv = BashOperator(
    task_id='GV_data_to_tsv',
    dag=pipeline,
    bash_command=("python "
    + "{SCRIPTS_FOLDER}GV_data_to_table.py "
    + "--file {DATA_FOLDER}kym_vision.json.gz "
    + "--out {OUTPUT_FOLDER}meme_safeness_relations.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, DATA_FOLDER=DATA_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

safeness_dim_tsv = BashOperator(
    task_id='safeness_dim_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}dim_safeness_gv.py "
                + "--file {OUTPUT_FOLDER}meme_safeness_relations.tsv "
                + "--out {OUTPUT_FOLDER}dim_safeness_gv.tsv")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER),
    trigger_rule='all_success',
    depends_on_past=False,
)

KYM_fact_table_tsv = BashOperator(
    task_id='KYM_fact_table_tsv',
    dag=pipeline,
    bash_command=("python "
                + "{SCRIPTS_FOLDER}fact_table_maker_augmented.py "
                + "--folder {OUTPUT_FOLDER} "
                + "--out {OUTPUT_FOLDER}fact_table_memes.tsv ")
    .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
            OUTPUT_FOLDER=OUTPUT_FOLDER),
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

def _postgres_populate_db(output_folder: str, postgres_conn_id: str):
    hook = PostgresHook.get_hook(postgres_conn_id)

    tables = ['dim_dates', 'dim_status', 'dim_origins', 'dim_safeness_gv', 'fact_table_memes']
    for table_name in tables:
        tmp_table = f'{output_folder}/temp_{table_name}.tsv'
        df_temp = pd.read_csv(
            f'{output_folder}{table_name}.tsv', sep="\t")
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
    },
    trigger_rule='all_success',
)


# lets use precalculated tsv, as this script runs for ca 10 minutes
if 0:
    make_meme_similarity_facts_csv = BashOperator(
        task_id='make_meme_similarity_facts_csv',
        dag=pipeline,
        bash_command= ("python "
            + " {SCRIPTS_FOLDER}memes_similarity_facts.py "
            + " --file {INPUT_FOLDER}kym_unique_filter_1.json "
            + " --threshold=0.5 "
            + " --out {OUTPUT_FOLDER}memes_similarity_score.tsv"
            ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, INPUT_FOLDER=OUTPUT_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER),
        trigger_rule='all_success',
        depends_on_past=False,
    )
else:
    make_meme_similarity_facts_csv = BashOperator(
        task_id='make_meme_similarity_facts_csv',
        dag=pipeline,
        bash_command= ("echo \"use precalculated none_failed\""
            ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, INPUT_FOLDER=OUTPUT_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER),
        trigger_rule='all_success',
        depends_on_past=False,
    )

extract_lda_topics = BashOperator(
    task_id='extract_lda_topics',
    dag=pipeline,
    bash_command = ("python "
        + " {SCRIPTS_FOLDER}lda_topics.py "
        + " --file {INPUT_FOLDER}kym_unique_filter_1.json "
        + " --outtopics {OUTPUT_FOLDER}lda_topics.tsv "
        + " --outmemes {OUTPUT_FOLDER}memes_lda_topics.tsv "
        + " --outkeywords {OUTPUT_FOLDER}lda_topic_keywords.tsv"
        ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER, INPUT_FOLDER=OUTPUT_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER),
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


neo4j_create_meme_nodes = Neo4jOperator(
    task_id='neo4j_create_Meme_nodes',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql= (" USING PERIODIC COMMIT 1000  \n "
          +  " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}memes.tsv\"  AS row FIELDTERMINATOR '\t' WITH row \n "
          + " MERGE (m:Meme {{Id: row.Id, category:row.category, status:row.details_status, description:row.meta_description, url:row.url}}) "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER),
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
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}parent_children_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n"
        + " MERGE (m:Meme {{Id: row.meme_Id}}) \n"
        + " MERGE (n:Meme {{Id: row.child}}) \n "
        + " CREATE (m)-[r:HAS_CHILD]->(n)  \n "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

#TODO save only meme1_Id>meme2_Id
neo4j_create_meme_sibling_relations = Neo4jOperator(
    task_id='neo4j_create_meme_sibling_relations',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}siblings_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:Meme {{Id: row.meme_Id}})  \n "
        + " MERGE (n:Meme {{Id: row.sibling}})  \n "
        + " CREATE (m)-[r:HAS_SIBLING]->(n)  \n "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER),
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

#nodes of text - keywords and tags
neo4j_create_meme_Text_has_tag_rel = Neo4jOperator(
    task_id='neo4j_create_meme_Text_has_tag_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER1}meme_tags.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (m:Meme {{Id: row.key_KYM}})  \n "
        + " MERGE (n:Text {{Id: row.tag}})  \n "
        + " CREATE (m)-[r:HAS_TAG]->(n)  \n "
        ).format(INPUT_FOLDER1=NEO4J_INPUT_FOLDER),
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

neo4j_create_Meme_LDAtopic_has_topic_rel = Neo4jOperator(
    task_id='neo4j_create_Meme_LDAtopic_has_topic_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=("USING PERIODIC COMMIT 1000  \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}memes_lda_topics.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
        + " MERGE (l:LDATopic {{Id: row.topic_Id}})  \n "
        + " MERGE (m:Meme {{Id: row.meme_Id}})  \n "
        + " CREATE (m)-[r:HAS_TOPIC{{score:toFloat(row.topic_prob_score)}}]->(l)  \n "
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

neo4j_create_Meme_has_common_tags_rel = Neo4jOperator(
    task_id='neo4j_create_Meme_has_common_tags_rel',
    neo4j_conn_id=NEO4J_CONN_ID,
    sql=(" USING PERIODIC COMMIT 100000 \n "
        + " LOAD CSV WITH HEADERS FROM \"file:///{INPUT_FOLDER}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
        + " WHERE toInteger(row.common_tags) >= 5 \n "
        + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
        + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
        + " CREATE (m)-[r:HAS_COMMON_TAGS{{nr: toInteger(row.common_tags)}}]->(n) \n "
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

neo4j_queries_to_tsv = DummyOperator(
    task_id='neo4j_queries_to_tsv',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)

#CALL apoc.export.csv.query("MATCH (a:Person)
#RETURN a.name AS name", "result.csv", {})

def _neo4j_make_query(ti, output_folder, query, output_file):
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

    neo4j_hook.close()


#MOST TAGS IN COMMON
neo4j_make_query1 = PythonOperator(
    task_id='neo4j_make_query1',
    dag=pipeline,
    python_callable=_neo4j_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'neoj4_queryTop100MemesWithTagsInCommon.tsv',
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
neo4j_make_query2 = PythonOperator(
    task_id='neo4j_make_query2',
    dag=pipeline,
    python_callable=_neo4j_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'neoj4_queryVerySimilarMemesNoTagsInCommon.tsv',
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

neo4j_make_query3 = PythonOperator(
    task_id='neo4j_make_query3',
    dag=pipeline,
    python_callable=_neo4j_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'neoj4_queryTop10MostDescendants.tsv',
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
neo4j_make_query4 = PythonOperator(
    task_id='neo4j_make_query4',
    dag=pipeline,
    python_callable=_neo4j_make_query,
    op_kwargs = {
        'output_folder' : OUTPUT_FOLDER,
        'output_file': OUTPUT_FOLDER + 'neoj4_queryTop10LongestParentChildChains.tsv',
        'query': """
            MATCH path = (:Meme{category:'Meme'})-[:HAS_CHILD*]->(next)
            WHERE not((next)-[:HAS_CHILD]->()) AND length(path)>4
            RETURN [t in nodes(path) | t.Id] AS chain
            ORDER BY length(path) DESC
            """,
    },
    depends_on_past=False,
)
queries_to_tsv = DummyOperator(
    task_id='queries_to_tsv',
    dag=pipeline,
    trigger_rule='all_success',
    depends_on_past=False,
)


plots = DummyOperator(
    task_id='plots',
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
    KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> queries_to_tsv >> plots >> sink

KYM_data_to_tsv >> [extract_lda_topics, make_meme_similarity_facts_csv] >> neo4j_delete_all_from_db >> neo4j_start_creating_indexes

for node in ['LDATopic', 'Meme', 'Text']:
    neo4j_create_index_node = Neo4jOperator(
        task_id="neo4j_create_index_"+node+"_node",
        neo4j_conn_id=NEO4J_CONN_ID,
        sql= (
            "CREATE BTREE INDEX {node}_Id IF NOT EXISTS FOR (n:{node}) ON (n.Id); "
            ).format(node=node),
        dag=pipeline,
        trigger_rule='all_success',
        depends_on_past=False,
    )
    neo4j_start_creating_indexes >> neo4j_create_index_node
    neo4j_create_index_node >> neo4j_start_creating_nodes

neo4j_start_creating_relations >>  \
    [neo4j_create_Meme_LDAtopic_has_topic_rel, neo4j_create_Meme_has_similar_description_rel, neo4j_create_Meme_has_similar_tags_rel, neo4j_create_meme_Text_has_tag_rel, neo4j_create_LDAtopic_Text_has_keyword_rel, neo4j_create_meme_child_relations, neo4j_create_meme_sibling_relations, neo4j_create_Meme_has_common_tags_rel]  \
    >> neo4j_queries_to_tsv >> [neo4j_make_query1, neo4j_make_query2, neo4j_make_query3, neo4j_make_query4] >> sink

source >> Google_Vision_data >> GV_data_to_tsv >> safeness_dim_tsv >> \
    KYM_fact_table_tsv >> db_init >> postgres_db >> populate_db >> queries_to_tsv >> plots >> sink
