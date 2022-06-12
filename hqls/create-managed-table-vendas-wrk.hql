CREATE DATABASE IF NOT EXISTS ${TARGET_DATABASE};

DROP TABLE IF EXISTS ${TARGET_DATABASE}.${TARGET_TABLE};

CREATE TABLE ${TARGET_DATABASE}.${TARGET_TABLE} (
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
PARTITIONED BY (DT_FOTO STRING)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat' 
TBLPROPERTIES ( 'orc.compress'='SNAPPY')
;