CREATE EXTERNAL TABLE datapath_health_raw.medical_research
(
    cord_uid         STRING,
    sha              STRING,
    source_x         STRING,
    title            STRING,
    doi              STRING,
    pmcid            STRING,
    pubmed_id        STRING,
    license          STRING,
    abstract         STRING,
    publish_time     STRING,
    authors          STRING,
    journal          STRING,
    mag_id           STRING,
    who_covidence_id STRING,
    arxiv_id         STRING,
    pdf_json_files   STRING,
    pmc_json_files   STRING,
    url              STRING,
    s2_id            STRING
)
    COMMENT 'Medical research table'
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            'separatorChar' = ',',
            'quoteChar' = '"'
        )
    LOCATION 's3://<BUCKET>/raw/medicalresearch/'
    TBLPROPERTIES ('skip.header.line.count' = '1', 'data_team' = 'dataeng', 'data_steward' = 'health_area');

CREATE EXTERNAL TABLE datapath_health_stage.medical_research_curated
(
    cord_uid         VARCHAR(8),
    sha              VARCHAR(256),
    source_x         VARCHAR(3),
    title            STRING,
    doi              VARCHAR(64),
    pmcid            VARCHAR(10),
    pubmed_id        INTEGER,
    license          VARCHAR(11),
    abstract         STRING,
    publish_time     DATE,
    authors          STRING,
    journal          VARCHAR(64),
    mag_id           STRING,
    who_covidence_id STRING,
    arxiv_id         STRING,
    pdf_json_files   STRING,
    pmc_json_files   STRING,
    url              VARCHAR(128),
    s2_id            STRING
)
    PARTITIONED BY (dt_month INTEGER)
    STORED AS PARQUET
    LOCATION 's3://<BUCKET>/stage/medicalresearch_curated/'
    TBLPROPERTIES ('parquet.compression' = 'SNAPPY', 'data_team' = 'dataeng', 'data_steward' = 'health_area');

CREATE EXTERNAL TABLE datapath_health_analytics.medical_research_word_count
(
    cord_uid         VARCHAR(8),
    word            STRING,
    count           INTEGER
)
    PARTITIONED BY (dt_month INTEGER)
    STORED AS PARQUET
    LOCATION 's3://<BUCKET>/analytics/medicalresearch_word_count/'
    TBLPROPERTIES ('parquet.compression' = 'SNAPPY', 'data_team' = 'dataeng', 'data_steward' = 'health_area');