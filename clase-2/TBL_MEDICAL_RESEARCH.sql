CREATE EXTERNAL TABLE datapath_demo.medical_research
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