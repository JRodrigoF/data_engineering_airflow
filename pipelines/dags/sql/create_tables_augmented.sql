/* Drop database if already exists*/

-- REVOKE CONNECT ON DATABASE memes FROM public;

-- SELECT pg_terminate_backend(pg_stat_activity.pid)
-- FROM pg_stat_activity
-- WHERE pg_stat_activity.datname = 'memes';

/* Create database */

-- DROP DATABASE IF EXISTS memes;

-- CREATE DATABASE IF NOT EXISTS memes
--     WITH
--     ENCODING = 'UTF8'
--     CONNECTION LIMIT = -1;
--     -- OWNER = root
--     -- LC_COLLATE = 'en_US.utf8'
--     -- LC_CTYPE = 'en_US.utf8'
--     -- TABLESPACE = pg_default

/* Create tables*/

-- CREATE TABLE films (
--     code        char(5) CONSTRAINT firstkey PRIMARY KEY,
--     title       varchar(40) NOT NULL,
--     did         integer NOT NULL,
--     date_prod   date,
--     kind        varchar(10),
--     len         interval hour to minute

-- CREATE TABLESPACE dbspace LOCATION '/data/dbs';
-- https://www.postgresql.org/docs/9.0/sql-createtablespace.html

DROP TABLE IF EXISTS fact_table_memes;
DROP TABLE IF EXISTS dim_status;
DROP TABLE IF EXISTS dim_origins;
DROP TABLE IF EXISTS dim_safeness_gv;
DROP TABLE IF EXISTS dim_dates;

CREATE TABLE dim_dates (
    date_Id int GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    timestamp int,
    year int,
    month varchar,
    day varchar,
    weekday varchar
);

CREATE TABLE dim_status (
    status_Id int GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    details_status varchar
);

CREATE TABLE dim_origins (
    origin_Id int GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    details_origin varchar
);

CREATE TABLE dim_safeness_gv (
    safeness_Id int GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    safeness_category varchar
);

CREATE TABLE fact_table_memes (
    Id int GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
    date_added_Id int NOT NULL,
    date_updated_Id int NOT NULL,
    safeness_Id int NOT NULL,
    status_Id int NOT NULL,
    origin_Id int NOT NULL,
    has_parent int NOT NULL,
    children_total int NOT NULL,
    content_variations_total int NOT NULL,
    tags_total int NOT NULL,
    FOREIGN KEY(date_added_Id)
        REFERENCES dim_dates(date_Id) MATCH SIMPLE,
    FOREIGN KEY(date_updated_Id)
        REFERENCES dim_dates(date_Id) MATCH SIMPLE,
    FOREIGN KEY(origin_Id)
        REFERENCES dim_origins(origin_Id) MATCH SIMPLE,
    FOREIGN KEY(status_Id)
        REFERENCES dim_status(status_Id) MATCH SIMPLE,
    FOREIGN KEY(safeness_Id)
        REFERENCES dim_safeness_gv(safeness_Id) MATCH SIMPLE
);

CREATE INDEX dates_idx ON dim_dates USING btree (date_Id);
CREATE INDEX status_idx ON dim_status USING btree (status_Id);
CREATE INDEX origins_idx ON dim_origins USING btree (origin_Id);
CREATE INDEX safeness_gv_idx ON dim_safeness_gv USING btree (safeness_Id);
CREATE INDEX ft_idx ON fact_table_memes USING btree (Id);

-- COPY [ BINARY ] table_name [ WITH OIDS ]
--      FROM { 'filename' | stdin }
--      [ [USING] DELIMITERS 'delimiter' ]
--      [ WITH NULL AS 'null_string' ]

--command " "\\copy public.dimdates (date_id, \"timestamp\", year, month, day, weekday) FROM '<STORAGE_DIR>/dim_dates.tsv' DELIMITER E'\\t' CSV HEADER QUOTE '\"' ESCAPE '''';""

-- COPY dim_dates FROM '/dim_dates.tsv' DELIMITER E'\t' CSV HEADER;
-- COPY dim_dates FROM '/dim_dates.tsv' DELIMITER E'\t' CSV HEADER;
-- COPY dim_status FROM '/dim_status.tsv' DELIMITER E'\t' CSV HEADER;
-- COPY dim_origins FROM '/dim_origins.tsv' DELIMITER E'\t' CSV HEADER;
-- COPY dim_safeness_gv FROM '/dim_safeness.tsv' DELIMITER E'\t' CSV HEADER;
-- COPY fact_table_memes FROM '/fact_table_memes.tsv' DELIMITER E'\t' CSV HEADER;

-- SELECT * FROM dimdates
-- ORDER BY date_id ASC

-- DROP TABLE dimdates
-- DROP TABLE dimorigins
-- DROP TABLE dim_safenessgv
-- DROP TABLE dimstatus
-- DROP TABLE meme_facts

-- docker ps
-- chmod 666 *.tsv
-- docker cp dim_dates.tsv postgres:/dim_dates.tsv
-- docker cp dim_origins.tsv postgres:/dim_origins.tsv
-- docker cp dim_safeness.tsv postgres:/dim_safeness.tsv
-- docker cp dim_status.tsv postgres:/dim_status.tsv
-- docker cp fact_table_memes.tsv postgres:/fact_table_memes.tsv
