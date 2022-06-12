SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE}

PARTITION(DT_FOTO) 
SELECT
	`Actual Delivery Date`,
	`CustomerKey`,
    `DateKey`,
    `Discount Amount`,
    `Invoice Date`,
    `Invoice Number`,
    `Item Class`,
    `Item Number`,
    `Item`,
    `Line Number`,
    `List Price`,
    `Order Number`,
    `Promised Delivery Date`,
    `Sales Amount`,
    `Sales Amount Based on List Price`,
    `Sales Cost Amount`,
    `Sales Margin Amount`,
    `Sales Price`,
    `Sales Quantity`,
    `Sales Rep`,
    `U/M`,
	'${DT_FOTO}' as DT_FOTO
FROM ${STAGE_DATABASE}.${STAGE_TABLE}
;