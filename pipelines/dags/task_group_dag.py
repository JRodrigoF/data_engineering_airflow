"""Example DAG demonstrating the usage of the TaskGroup."""
import pandas as pd
import os
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook

DAGS_FOLDER = '/opt/airflow/dags/'
SCRIPTS_FOLDER = DAGS_FOLDER + 'scripts/'
DATA_FOLDER = DAGS_FOLDER + 'data/'

# each pipe taskgroup will write it's ouput to different folder
TASK1_FOLDER = DAGS_FOLDER + 'output/pipe_1/'
TASK2_FOLDER = DAGS_FOLDER + 'output/pipe_2/'
TASK3_FOLDER = DAGS_FOLDER + 'output/pipe_3/'
TASK4_FOLDER = DAGS_FOLDER + 'output/pipe_4/'

POSTGRES_CONN_ID = "postgres_default"
NEO4J_CONN_ID = "neo4j_default"

# lets use by default precalculated meme similarity score tsv, as this script runs for ca 7-10 minutes
USE_PRECALCULATED_MEMES_SIMILARITY_DATA = 1

# [START howto_task_group]
with DAG(dag_id="pipeline_task_group", start_date=days_ago(2), tags=["memes"]) as pipeline:
    source = DummyOperator(task_id="source")

    # [START howto_task_group_section_1]

    with TaskGroup("pipe_1", tooltip="Tasks for pipe_1") as pipe_1:
        OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipe_1/'
        start = DummyOperator(
            task_id='start',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        make_output_folder = BashOperator(
            task_id='make_output_folder',
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
        end = DummyOperator(
            task_id='end',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        start >> make_output_folder >> KYM_data >> remove_duplicates >> filtering >> KYM_data_to_tsv >> rename_output_files >> end

    ######################################## END PIPE_1 ###################################

    ######################################## START PIPE_2 ###################################
    with TaskGroup("pipe_2", tooltip="Tasks for pipe_2") as pipe_2:
        OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipe_2/'

        start = DummyOperator(
            task_id='start',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        make_output_folder = BashOperator(
            task_id='make_output_folder',
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

        end = DummyOperator(
            task_id='end',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        start >> make_output_folder >> Google_Vision_data >> GV_data_to_tsv >> safeness_dim_tsv >> rename_output_files >> end

    ######################################## END PIPE_2 ###################################

    ######################################## START PIPE_3 ###################################

    with TaskGroup("pipe_3", tooltip="Tasks for pipe_3") as pipe_3:
        OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipe_3/'

        start = DummyOperator(
            task_id='start',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        make_output_folder = BashOperator(
            task_id='make_output_folder',
            dag=pipeline,
            bash_command=("mkdir -p  {OUTPUT_FOLDER}").format(OUTPUT_FOLDER=OUTPUT_FOLDER),
            trigger_rule='none_failed'
        )

        memes_dim_tsv = BashOperator(
            task_id='memes_dim_tsv',
            dag=pipeline,
            bash_command=("python "
                        + " {SCRIPTS_FOLDER}make_memes_dim_tsv.py "
                        + "--file {TASK1_FOLDER}kym_unique_filter_1.json "
                        + "--outfile {OUTPUT_FOLDER}{epoch}_memes_dim.tsv ")
            .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                    OUTPUT_FOLDER=OUTPUT_FOLDER,
                    TASK1_FOLDER=TASK1_FOLDER,
                    epoch="{{ execution_date.int_timestamp }}"),
            trigger_rule='all_success',
            depends_on_past=False,
        )

        status_dim_tsv = BashOperator(
            task_id='status_dim_tsv',
            dag=pipeline,
            bash_command=("python "
                        + " {SCRIPTS_FOLDER}dim_status.py "
                        + "--file {TASK1_FOLDER}memes.tsv "
                        + "--out {OUTPUT_FOLDER}{epoch}_dim_status.tsv")
            .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                    TASK1_FOLDER=TASK1_FOLDER,
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
                        + "--file {TASK1_FOLDER}memes.tsv "
                        + "--out {OUTPUT_FOLDER}{epoch}_dim_dates.tsv")
            .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                    TASK1_FOLDER=TASK1_FOLDER,
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
                        + "--file {TASK1_FOLDER}memes.tsv "
                        + "--out {OUTPUT_FOLDER}{epoch}_dim_origins.tsv")
            .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                    TASK1_FOLDER=TASK1_FOLDER,
                    OUTPUT_FOLDER=OUTPUT_FOLDER,
                    epoch="{{ execution_date.int_timestamp }}"),
            trigger_rule='all_success',
            depends_on_past=False,
        )

        KYM_fact_table_tsv = BashOperator(
            task_id='KYM_fact_table_tsv',
            dag=pipeline,
            bash_command=("python "
                        + "{SCRIPTS_FOLDER}fact_table_maker_augmented.py "
                        + "--folder1 {TASK1_FOLDER} "
                        + "--folder2 {TASK2_FOLDER} "
                        + "--folder3 {OUTPUT_FOLDER} "
                        + "--prefix {epoch} "
                        + "--out {OUTPUT_FOLDER}{epoch}_fact_table_memes.tsv ")
            .format(SCRIPTS_FOLDER=SCRIPTS_FOLDER,
                    TASK1_FOLDER=TASK1_FOLDER,
                    TASK2_FOLDER=TASK2_FOLDER,
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

        def _postgres_populate_db(output_folder: str, task2_folder: str, postgres_conn_id: str, epoch: str):
            hook = PostgresHook.get_hook(postgres_conn_id)

            tables = ['dim_dates', 'dim_status', 'dim_origins', 'dim_safeness_gv', 'fact_table_memes']
            for table_name in tables:
                tmp_table = f'{output_folder}/temp_{epoch}_{table_name}.tsv'
                if table_name == 'dim_safeness_gv':
                    df_temp = pd.read_csv(
                        f'{task2_folder}/{epoch}_{table_name}.tsv', sep="\t")
                else:
                    df_temp = pd.read_csv(
                        f'{output_folder}/{epoch}_{table_name}.tsv', sep="\t")
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
                'task2_folder': TASK2_FOLDER,
                'postgres_conn_id': POSTGRES_CONN_ID,
                'epoch': "{{ execution_date.int_timestamp }}"
            },
            trigger_rule='all_success',
        )

        def _queries_to_tables(dags_folder: str, output_folder: str,
            postgres_conn_id: str, query_names: list, epoch = str):

            # hook = PostgresHook.get_hook(postgres_conn_id)
            conn = PostgresHook(postgres_conn_id=postgres_conn_id).get_conn()

            sql_folder = '{}sql'.format(dags_folder)
            sql_files = sorted([f for f in os.listdir(sql_folder) if
                f.startswith('query') and f.endswith('.sql')])

            sql_query_params = {'query_04': {'category': 'racy'}}

            for sql_file in sql_files:
                file_path = '{}/{}'.format(sql_folder, sql_file)
                file_preffix = sql_file.replace('.sql', '')
                with open(file_path, 'r') as f:
                    sql_query = f.read().replace('\'', '')

                if file_preffix in sql_query_params:
                    # df = pd.read_sql_query(sql_query, conn, params=sql_query_params[file_preffix])
                    df = pd.read_sql(sql_query, conn, params={'category': 'racy'})
                else:
                    df = pd.read_sql(sql_query, conn)
                out_file = f'{output_folder}/{epoch}_{file_preffix}.tsv'
                df.to_csv(out_file, sep="\t", encoding='utf-8', na_rep='None')

        queries_to_tsv = PythonOperator(
            task_id='queries_to_tsv',
            dag=pipeline,
            python_callable=_queries_to_tables,
            op_kwargs={
                'dags_folder': DAGS_FOLDER,
                'output_folder': OUTPUT_FOLDER,
                'postgres_conn_id': POSTGRES_CONN_ID,
                'query_names': [],
                'epoch': "{{ execution_date.int_timestamp }}"},
            trigger_rule='all_success',
        )

        make_plots = DummyOperator(
            task_id='make_plots',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        files = ['fact_table_memes.tsv', 'dim_origins.tsv', 'dim_dates.tsv', \
            'dim_status.tsv', 'memes_dim.tsv', \
            'query_01.tsv', 'query_02.tsv', 'query_03.tsv', 'query_04.tsv']
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

        end = DummyOperator(
            task_id='end',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        start >> make_output_folder >> [date_dim_tsv, status_dim_tsv, \
            origin_dim_tsv, memes_dim_tsv] >> KYM_fact_table_tsv >> db_init >> \
                postgres_db >> populate_db >> queries_to_tsv >> make_plots >> \
                    rename_output_files >> end

    ######################################## END PIPE_3 ###################################

    ######################################## START PIPE_4 ###################################

    with TaskGroup("pipe_4", tooltip="Tasks for pipe_4") as pipe_4:
        OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipe_4/'

        start = DummyOperator(
            task_id='start',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        make_output_folder = BashOperator(
            task_id='make_output_folder',
            dag=pipeline,
            bash_command=("mkdir -p  {OUTPUT_FOLDER}").format(OUTPUT_FOLDER=OUTPUT_FOLDER),
            trigger_rule='none_failed'
        )

        # lets use precalculated tsv, as this script runs for ca 7 minutes
        if USE_PRECALCULATED_MEMES_SIMILARITY_DATA:
            calculate_memes_similarity = BashOperator(
                task_id='calculate_memes_similarity',
                dag=pipeline,
                bash_command= ("gunzip -c {DATA_FOLDER}precalculated_memes_similarity_score.tsv.gz > {OUTPUT_FOLDER}{epoch}_memes_similarity_score.tsv"
                    ).format(DATA_FOLDER=DATA_FOLDER, OUTPUT_FOLDER=OUTPUT_FOLDER, epoch="{{ execution_date.int_timestamp }}"),
                trigger_rule='all_success',
                depends_on_past=False,
            )
        else:
            calculate_memes_similarity = BashOperator(
                task_id='calculate_memes_similarity',
                dag=pipeline,
                bash_command= ("python "
                    + " {SCRIPTS_FOLDER}memes_similarity_facts.py "
                    + " --file {TASK1_FOLDER}kym_unique_filter_1.json "
                    + " --threshold=0.5 "
                    + " --out {OUTPUT_FOLDER}{epoch}_memes_similarity_score.tsv"
                    ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER
                        , TASK1_FOLDER=TASK1_FOLDER
                        , OUTPUT_FOLDER=OUTPUT_FOLDER
                        , epoch="{{ execution_date.int_timestamp }}"),
                trigger_rule='all_success',
                depends_on_past=False,
            )

        extract_lda_topics = BashOperator(
            task_id='extract_lda_topics',
            dag=pipeline,
            bash_command = ("python "
                + " {SCRIPTS_FOLDER}lda_topics.py "
                + " --file {TASK1_FOLDER}kym_unique_filter_1.json "
                + " --outtopics {OUTPUT_FOLDER}{epoch}_lda_topics.tsv "
                + " --outmemes {OUTPUT_FOLDER}{epoch}_memes_lda_topics.tsv "
                + " --outkeywords {OUTPUT_FOLDER}{epoch}_lda_topic_keywords.tsv"
                ).format(SCRIPTS_FOLDER=SCRIPTS_FOLDER
                    , TASK1_FOLDER=TASK1_FOLDER
                    , OUTPUT_FOLDER=OUTPUT_FOLDER
                    , epoch="{{ execution_date.int_timestamp }}"),
            trigger_rule='all_success',
            depends_on_past=False,
        )

        files = ['memes_similarity_score.tsv'
            , 'lda_topics.tsv'
            , 'lda_topic_keywords.tsv'
            , 'memes_lda_topics.tsv'
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

        end = DummyOperator(
            task_id='end',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        start >> make_output_folder >> [calculate_memes_similarity, extract_lda_topics] >> rename_output_files >> end

    ######################################## END PIPE_4 ###################################

    ######################################## START PIPE_5 ###################################

    with TaskGroup("pipe_5", tooltip="Tasks for pipe_5") as pipe_5:
        OUTPUT_FOLDER = DAGS_FOLDER + 'output/pipe_5/'
        TASK1_FOLDER_NEO4J = 'pipe_1/'
        TASK4_FOLDER_NEO4J = 'pipe_4/'

        start = DummyOperator(
            task_id='start',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        make_output_folder = BashOperator(
            task_id='make_output_folder',
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

        ############## START PIPE_5 SUB_TASK populate_db ########

        with TaskGroup("populate_db", tooltip="Tasks for populating Neoj4 db nodes and relations") as populate_neoj4_db:

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
                      +  " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER_NEO4J}memes.tsv\"  AS row FIELDTERMINATOR '\t' WITH row \n "
                      + " MERGE (m:Meme {{Id: row.Id, category:row.category, status:row.details_status, description:row.meta_description, url:row.url}}) "
                    ).format(TASK1_FOLDER_NEO4J=TASK1_FOLDER_NEO4J),
                dag=pipeline,
                trigger_rule='all_success',
                depends_on_past=False,
            )

            create_LDAtopics = Neo4jOperator(
                task_id='create_LDAtopics',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql= (" LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER_NEO4J}lda_topics.tsv\" AS row FIELDTERMINATOR '\t' \n"
                    + " MERGE (m:LDATopic {{Id: row.Id, name:row.name}}) "
                    ).format(TASK4_FOLDER_NEO4J=TASK4_FOLDER_NEO4J),
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
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER_NEO4J}parent_children_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n"
                    + " MERGE (m:Meme {{Id: row.meme_Id}}) \n"
                    + " MERGE (n:Meme {{Id: row.child}}) \n "
                    + " CREATE (m)-[r:HAS_CHILD]->(n)  \n "
                    ).format(TASK1_FOLDER_NEO4J=TASK1_FOLDER_NEO4J),
                dag=pipeline,
                trigger_rule='all_success',
                depends_on_past=False,
            )

            #TODO save only meme1_Id>meme2_Id
            Meme_siblings = Neo4jOperator(
                task_id='Meme_siblings',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql=("USING PERIODIC COMMIT 1000  \n "
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER_NEO4J}siblings_relations.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
                    + " MERGE (m:Meme {{Id: row.meme_Id}})  \n "
                    + " MERGE (n:Meme {{Id: row.sibling}})  \n "
                    + " CREATE (m)-[r:HAS_SIBLING]->(n)  \n "
                    ).format(TASK1_FOLDER_NEO4J=TASK1_FOLDER_NEO4J),
                dag=pipeline,
                trigger_rule='all_success',
                depends_on_past=False,
            )

            #nodes of text - keywords and tags
            Meme_Tags = Neo4jOperator(
                task_id='Meme_Tags',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql=("USING PERIODIC COMMIT 1000  \n "
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK1_FOLDER_NEO4J}meme_tags.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
                    + " MERGE (m:Meme {{Id: row.key_KYM}})  \n "
                    + " MERGE (n:Text {{Id: row.tag}})  \n "
                    + " CREATE (m)-[r:HAS_TAG]->(n)  \n "
                    ).format(TASK1_FOLDER_NEO4J=TASK1_FOLDER_NEO4J),
                dag=pipeline,
                trigger_rule='all_success',
                depends_on_past=False,
            )

            LDAtopic_Keywords = Neo4jOperator(
                task_id='LDAtopic_Keywords',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql=("USING PERIODIC COMMIT 1000  \n "
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER_NEO4J}lda_topic_keywords.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
                    + " MERGE (m:LDATopic {{Id: row.topic_Id}})  \n "
                    + " MERGE (n:Text {{Id: row.keyword}})  \n "
                    + " CREATE (m)-[r:HAS_KEYWORD{{score:toFloat(row.keyword_prob_scores)}}]->(n)  \n "
                    ).format(TASK4_FOLDER_NEO4J=TASK4_FOLDER_NEO4J),
                dag=pipeline,
                trigger_rule='all_success',
                depends_on_past=False,
            )

            Meme_LDAtopics = Neo4jOperator(
                task_id='Meme_LDAtopics',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql=("USING PERIODIC COMMIT 1000  \n "
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER_NEO4J}memes_lda_topics.tsv\" AS row FIELDTERMINATOR '\t' WITH row  \n "
                    + " MERGE (l:LDATopic {{Id: row.topic_Id}})  \n "
                    + " MERGE (m:Meme {{Id: row.meme_Id}})  \n "
                    + " CREATE (m)-[r:HAS_TOPIC{{score:toFloat(row.topic_prob_score)}}]->(l)  \n "
                    ).format(TASK4_FOLDER_NEO4J=TASK4_FOLDER_NEO4J),
                dag=pipeline,
                trigger_rule='all_success',
                depends_on_past=False,
            )

            Meme_has_similar_desc = Neo4jOperator(
                task_id='Meme_has_similar_desc',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql=(" USING PERIODIC COMMIT 100000 \n "
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER_NEO4J}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
                    + " WHERE toFloat(row.desc_similarity) >= 0.2 \n "
                    + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
                    + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
                    + " CREATE (m)-[r:HAS_SIMILAR_DESC{{score: toFloat(row.desc_similarity)}}]->(n) \n "
                    ).format(TASK4_FOLDER_NEO4J=TASK4_FOLDER_NEO4J),
                    dag=pipeline,
                    trigger_rule='all_success',
                    depends_on_past=False,
                )

            Meme_has_common_tags = Neo4jOperator(
                task_id='Meme_has_common_tags',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql=(" USING PERIODIC COMMIT 100000 \n "
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER_NEO4J}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
                    + " WHERE toInteger(row.common_tags) >= 5 \n "
                    + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
                    + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
                    + " CREATE (m)-[r:HAS_COMMON_TAGS{{nr: toInteger(row.common_tags)}}]->(n) \n "
                    ).format(TASK4_FOLDER_NEO4J=TASK4_FOLDER_NEO4J),
                    dag=pipeline,
                    trigger_rule='all_success',
                    depends_on_past=False,
                )

            Meme_has_similar_tags = Neo4jOperator(
                task_id='Meme_has_similar_tags',
                neo4j_conn_id=NEO4J_CONN_ID,
                sql=(" USING PERIODIC COMMIT 100000 \n "
                    + " LOAD CSV WITH HEADERS FROM \"file:///{TASK4_FOLDER_NEO4J}memes_similarity_score.tsv\" AS row FIELDTERMINATOR '\t' WITH row \n "
                    + " WHERE toFloat(row.tags_similarity) >= 0.2 \n "
                    + " MERGE (m:Meme {{Id: row.meme1_Id}}) \n "
                    + " MERGE (n:Meme {{Id: row.meme2_Id}}) \n "
                    + " CREATE (m)-[r:HAS_SIMILAR_TAGS{{score: toFloat(row.tags_similarity)}}]->(n) \n "
                    ).format(TASK4_FOLDER_NEO4J=TASK4_FOLDER_NEO4J),
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
                , Meme_has_similar_tags ]

        ############## END PIPE_5 SUB_TASK populate_db ########

        ############## START PIPE_5 SUB_TASK queries_to_tsv ########

        with TaskGroup("queries_to_tsv", tooltip="Tasks for populating Neoj4 db nodes and relations") as queries_neo4j_to_tsv:

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
            [query1, query2, query3, query4]

        ############## END PIPE_5 SUB_TASK queries_to_tsv ########

        end = DummyOperator(
            task_id='end',
            dag=pipeline,
            trigger_rule='none_failed'
        )

        start >> make_output_folder >> prepare_neo4j_db >> populate_neoj4_db  >> queries_neo4j_to_tsv  >> end

        ######################################## END PIPE_5 ###################################

    sink = DummyOperator(task_id='sink')

    #start >> pipe_1 >> pipe_2 >> pipe_3 >> pipe_4 >> pipe_5 >> end
    # alternative version
    source >> [pipe_1 , pipe_2] >> pipe_3 >> sink
    pipe_1 >> pipe_4 >> pipe_5 >> sink
