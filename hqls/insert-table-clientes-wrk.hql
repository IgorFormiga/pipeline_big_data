SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE
    ${TARGET_DATABASE}.${TARGET_TABLE}

PARTITION(DT_FOTO) 
SELECT
	`Address Number`,
	`Business Family`,
    `Business Unit`,
	`Customer`,
    `CustomerKey`,
	`Customer Type`,
    `Division`,
	`Line of Business`,
	`Phone`,
    `Region Code`,
    `Regional Sales Mgr`,
    `Search Type`,
	'${DT_FOTO}' as DT_FOTO
FROM ${STAGE_DATABASE}.${STAGE_TABLE}
;
