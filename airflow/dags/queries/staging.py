import os
from dotenv import load_dotenv 

# loading variables from .env file
load_dotenv()

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ATHENA_STAGING_DATABASE = os.getenv("AWS_ATHENA_STAGING_DATABASE")

STATION_TABLE_NAME = 'station'
STOP_TABLE_NAME = 'stop'
PUNCTUALITY_TABLE_NAME = 'punctuality'

QUERY_CREATE_EXTERNAL_STATION_TABLE_STAGING: str = f"""
CREATE EXTERNAL TABLE {AWS_ATHENA_STAGING_DATABASE}.{STATION_TABLE_NAME}_ext(
  `URI` string                    COMMENT 'The URI where we can find more information (such as the real-time departures) about this station (this already contains the ID of the NMBS/SNCB as well)',
  `name` string                   COMMENT 'The most neutral name of the station (e.g., in Wallonia use the French name, for Brussels use both, for Flanders use nl name)',
  `alternative-fr` string         COMMENT 'The alt. name in French, if available',
  `alternative-nl` string         COMMENT 'The alt. name in Dutch, if available',
  `alternative-de` string         COMMENT 'The alt. name in German, if available',
  `alternative-en` string         COMMENT 'The alt. name in English, if available',
  `taf-tap-code` string           COMMENT 'The code of the country the station belongs to',
  `telegraph-code` string         COMMENT 'The code of the country the station belongs to',
  `country-code` string           COMMENT 'The code of the country the station belongs to',
  `longitude` string              COMMENT 'The longitude of the station',
  `latitude` string               COMMENT 'The latitude of the station',
  `avg_stop_times` string         COMMENT 'The average number of vehicles stopping each day in this station (computed field)',
  `official_transfer_time` string COMMENT 'The time needed for an average person to make a transfer in this station, according to official sources (NMBS/SNCB) (computed field)'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'skip.header.line.count'='1')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://{AWS_BUCKET_NAME}/{STATION_TABLE_NAME}/csv/'
TBLPROPERTIES ('classification' = 'csv');
"""

QUERY_DROP_STATION_TABLE_STAGING : str = f"""
DROP TABLE IF EXISTS {AWS_ATHENA_STAGING_DATABASE}.{STATION_TABLE_NAME}_ext;
"""

QUERY_CREATE_EXTERNAL_STOP_TABLE_STAGING: str = f"""
CREATE EXTERNAL TABLE {AWS_ATHENA_STAGING_DATABASE}.{STOP_TABLE_NAME}_ext(
  `URI` string              COMMENT 'this is the URI where we can find more information (such as the real-time departures) about this station (this already contains the ID of the NMBS/SNCB as well)',
  `parent_stop` string      COMMENT 'the URI of the parent stop defined in stations.csv',
  `longitude` string        COMMENT 'The longitude of the stop',
  `latitude` string         COMMENT 'The latitude of the stop',
  `name` string             COMMENT 'The name of the stop',
  `alternative-fr` string   COMMENT 'The alt. name in French, if available',
  `alternative-nl` string   COMMENT 'The alt. name in Dutch, if available',
  `alternative-de` string   COMMENT 'The alt. name in German, if available',
  `alternative-en` string   COMMENT 'The alt. name in English, if available',
  `platform` string         COMMENT 'The platform code (can also consist of letters, so do not treat this as a number!)'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'skip.header.line.count'='1')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://{AWS_BUCKET_NAME}/{STOP_TABLE_NAME}/csv/'
TBLPROPERTIES ('classification' = 'csv');
"""

QUERY_DROP_STOP_TABLE_STAGING : str = f"""
DROP TABLE IF EXISTS {AWS_ATHENA_STAGING_DATABASE}.{STOP_TABLE_NAME}_ext;
"""

QUERY_CREATE_ICEBERG_PUNCTUALITY_TABLE_STAGING: str = f"""
CREATE TABLE IF NOT EXISTS {AWS_ATHENA_STAGING_DATABASE}.{PUNCTUALITY_TABLE_NAME}_iceberg(
  unique_row_id binary        COMMENT 'A unique identifier for the punctuality, generated by hashing key punctuality attributes',
  filename string             COMMENT 'The source filename from which the punctuality data was loaded',
  DATDEP string               COMMENT 'The departure date',
  TRAIN_NO int                COMMENT 'The train number',
  RELATION string             COMMENT 'The train type',
  TRAIN_SERV string           COMMENT 'The operator',
  PTCAR_NO int                COMMENT 'The measuring point number',
  THOP1_COD string ,
  LINE_NO_DEP int             COMMENT 'The start line',
  REAL_TIME_ARR string     COMMENT 'The actual arrival time',
  REAL_TIME_DEP string     COMMENT 'The actual departure time',
  PLANNED_TIME_ARR string  COMMENT 'The scheduled time of arrival',
  PLANNED_TIME_DEP string  COMMENT 'The scheduled time of departure',
  DELAY_ARR int               COMMENT 'The arrival delay',
  DELAY_DEP int               COMMENT 'The delayed departure',
  CIRC_TYP int ,
  RELATION_DIRECTION string   COMMENT 'The train direction',
  PTCAR_LG_NM_NL string       COMMENT 'The name of stopping point',
  LINE_NO_ARR int             COMMENT 'The arrival line',
  PLANNED_DATE_ARR string     COMMENT 'The estimated date of arrival',
  PLANNED_DATE_DEP string     COMMENT 'The scheduled departure date',
  REAL_DATE_ARR string        COMMENT 'The actual arrival date',
  REAL_DATE_DEP string        COMMENT 'The actual departure date'
)
COMMENT '{PUNCTUALITY_TABLE_NAME}_punctuality_data with description'
LOCATION 's3://{AWS_BUCKET_NAME}/{PUNCTUALITY_TABLE_NAME}/tables/iceberg'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_compression'='snappy'
);
"""