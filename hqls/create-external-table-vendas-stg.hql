CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE EXTERNAL TABLE IF NOT EXISTS ${TARGET_DATABASE}.${TARGET_TABLE}(
	`Actual Delivery Date` string,
	`CustomerKey` int,
    `DateKey` string,
    `Discount Amount` string,
    `Invoice Date` string,
    `Invoice Number` int,
    `Item Class` string,
    `Item Number` string,
    `Item` string,
    `Line Number` int,
    `List Price` string,
    `Order Number` int,
    `Promised Delivery Date` string,
    `Sales Amount` string,
    `Sales Amount Based on List Price` string,
    `Sales Cost Amount` string,
    `Sales Margin Amount` string,
    `Sales Price` string,
    `Sales Quantity` int,
    `Sales Rep` int,
    `U/M` string
)
COMMENT 'Tabela Externa de Divisao'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
STORED AS TEXTFILE
LOCATION '${HDFS_DIR}'
tblproperties ('skip.header.line.count'='1', 'store.charset'='UTF-8', 'retrieve.charset'='UTF-8');