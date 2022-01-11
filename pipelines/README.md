## Requirements

* To have docker *and* docker-compose installed.
* Install docker and docker-compose exactly as it is described in the website.
* **do not do do apt install docker or docker-compose**

## How to spin the Airflow webserver up

### Prepping

First, get your **id**:
```sh
id -u
```

Now edit the **.env** file and swap out 1000 for your own.

Run the following command to creat the volumes needed in order to send data to airflow:
```sh
mkdir -p ./logs ./plugins
```

Extract nltk_data.tar.gz
```sh
tar -xf nltk_data.tar.gz
```

Run this command **once**:

Note: This is important since it initializes the configuration for both Postgres and Neo4j connections so you do not have to do it manually in the Airflow webserver UI.
```sh
docker-compose up airflow-init
```
If the exit code is 0 then it's all good.

### Running

```sh
docker-compose up
```
Now you should be able to go to [http://localhost:8080](http://localhost:8080) using your browser and start using the UI

### Connections

Parameters used for Postgres and Neo4j db connections:

**postgres**

* Name - postgres_default
* Conn type - postgres
* Host - postgres
* Database - airflow
* Username - airflow
* Port - 5432

**neoj4**

* Name - neo4j_default
* Conn type - neo4j
* Host - neo4j
* Database - neo4j
* Username - neo4j
* Password - pass
* Port - 7687
