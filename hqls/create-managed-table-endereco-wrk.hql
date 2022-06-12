CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE TABLE ${TARGET_DATABASE}.${TARGET_TABLE} (
	`Address Number` int,
	`City` string,
    `Country` string,
	`Customer Address 1` string,
    `Customer Address 2` string,
	`Customer Address 3` string,
    `Customer Address 4` string,
	`State` string,
	`Zip Code` string
)
PARTITIONED BY (DT_FOTO STRING)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY')
;