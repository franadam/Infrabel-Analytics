import os
from dotenv import load_dotenv 

# loading variables from .env file
load_dotenv()

AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_ATHENA_STAGING_DATABASE = os.getenv("AWS_ATHENA_STAGING_DATABASE")

GEOGRAPHY_TABLE_NAME = 'geography'
PUNCTUALITY_TABLE_NAME = 'punctuality'

QUERY_CREATE_EXTERNAL_GEOGRAPHY_TABLE_STAGING: str = f"""
CREATE EXTERNAL TABLE {AWS_ATHENA_STAGING_DATABASE}.{GEOGRAPHY_TABLE_NAME}_ext(
  city          string COMMENT 'The name of the city/town as a Unicode string',
  city_ascii    string COMMENT 'The name of the city as an ASCII string',
  lat           double COMMENT 'The latitude of the city/town',
  lng           double COMMENT 'The longitude of the city/town',
  country       string COMMENT 'The name of the country',
  iso2          string COMMENT 'The alpha-2 iso code of the country',
  iso3          string COMMENT 'TLC Taxi Zone in which the taximeter was disengaged',
  admin_name    string COMMENT 'The name of the highest level administration region of the city town',
  capital       string COMMENT 'The name of the country s capital',
  population    int COMMENT 'An estimate of the city s urban population',
  id            int COMMENT 'A 10-digit unique id generated by SimpleMaps'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',', 'skip.header.line.count'='1')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://{AWS_BUCKET_NAME}/{GEOGRAPHY_TABLE_NAME}/csv/'
TBLPROPERTIES ('classification' = 'csv');
"""

QUERY_DROP_GEOGRAPHY_TABLE_STAGING : str = f"""
DROP TABLE IF EXISTS {AWS_ATHENA_STAGING_DATABASE}.{GEOGRAPHY_TABLE_NAME}_ext;
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