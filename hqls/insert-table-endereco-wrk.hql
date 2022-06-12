SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE}

PARTITION(DT_FOTO) 
SELECT
	`Address Number`,
	`City`,
    `Country`,
	`Customer Address 1`,
    `Customer Address 2`,
	`Customer Address 3`,
    `Customer Address 4`,
	`State`,
	`Zip Code`,
	'${DT_FOTO}' as DT_FOTO
FROM ${STAGE_DATABASE}.${STAGE_TABLE}
;