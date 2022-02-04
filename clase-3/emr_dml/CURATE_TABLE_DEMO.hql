SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.dynamic.partition = true;
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

INSERT INTO datapath_health_stage.medical_research_curated PARTITION (dt_month)
SELECT CAST(cord_uid AS VARCHAR(8)),
       CAST(sha AS VARCHAR(256)),
       CAST(source_x AS VARCHAR(3)),
       title,
       CAST(doi AS VARCHAR(64)),
       CAST(pmcid AS VARCHAR(10)),
       CAST(pubmed_id AS INTEGER),
       CAST(license AS VARCHAR(11)),
       abstract,
       FROM_UNIXTIME(UNIX_TIMESTAMP(publish_time, "MM/dd/yyyy")),
       authors,
       CAST(journal AS VARCHAR(64)),
       mag_id,
       who_covidence_id,
       arxiv_id,
       pdf_json_files,
       pmc_json_files,
       CAST(url AS VARCHAR(128)),
       s2_id,
       CAST(CONCAT(SPLIT(publish_time, '/')[2], SPLIT(publish_time, '/')[0]) AS INTEGER)
FROM datapath_health_raw.medical_research;