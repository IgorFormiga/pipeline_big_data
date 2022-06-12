#!/bin/bash

TARGET_DATABASE="desafio_final"
TARGET_TABLE="TBL_CLIENTES"
STAGE_TABLE="TBL_CLIENTES_STG" 
TARGET_DATABASE="desafio_final"
STAGE_DATABASE="desafio_final"
# INSERT_WRK="$2"


DT_FOTO="$(date "+%Y-%m-%d")"

beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 --hivevar STAGE_TABLE="${STAGE_TABLE}" \
 --hivevar STAGE_DATABASE="${STAGE_DATABASE}" \
 --hivevar DT_FOTO="${DT_FOTO}" \
 -f ../hqls/insert-table-clientes-wrk.hql