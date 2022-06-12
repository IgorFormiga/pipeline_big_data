CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE}(
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
COMMENT 'Tabela Externa de Clientes'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${HDFS_DIR}'
tblproperties ('skip.header.line.count'='1', 'store.charset'='UTF-8', 'retrieve.charset'='UTF-8')
;