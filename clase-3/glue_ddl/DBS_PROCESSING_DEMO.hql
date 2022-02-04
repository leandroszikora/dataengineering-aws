CREATE DATABASE IF NOT EXISTS datapath_health_raw
LOCATION 's3://<BUCKET>/raw/';

CREATE DATABASE IF NOT EXISTS datapath_health_stage
LOCATION 's3://<BUCKET>/stage/';

CREATE DATABASE IF NOT EXISTS datapath_health_analytics
LOCATION 's3://<BUCKET>/analytics/';