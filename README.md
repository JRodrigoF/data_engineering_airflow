# deng_g7
Data Engineering 2021 Project

Theme: Internet Memes

-----

Data source:
    [https://meme4.science/](https://meme4.science/)

Pipeline Overview

The complete pipeline is implemented as various sections or 'pipes', each of these with a specific task. The pipeline is written as a single Airflow DAG while each pipe in the DAG is an Airflow Task Group that runs either in series or in parallel with respect to other pipes. Each pipe depends only on the successful generation of its input data by the previous pipe. The only exception being the first pipe. The pipeline is composed of 5 different 'pipes', as illustrated in the figure below. The general goal of each pipe is the following:

- Pipe 1. Data Cleaning

- Pipe 2. Data Augmentation (Google Vision API)

- Pipe 3. Data Ingestion and Analysis (Relational Modeling: Postgres Data Warehouse)

- Pipe 4. Data Augmentation (LDA-based Topics and Meme's Similarity)

- Pipe 5. Data Ingestion and Analysis (Graph Modeling: Neo4j)

![pipelines](https://github.com/rabauti/deng_g7/blob/main/imgs/pipelines_overview.png)
