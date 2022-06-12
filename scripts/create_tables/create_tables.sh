#!/bin/bash

# Tabela CLIENTE
echo "Carregando Variáveis CLIENTES"
HDFS_DIR="/projeto/desafio_final/dados/CLIENTES"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TBL_CLIENTES_STG"
TARGET_TABLE="TBL_CLIENTES"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Criando a tabela Externa
echo "Criando tabela Externa TBL_CLIENTES_STG"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/create-external-table-clientes-stg.hql

# Criando a tabela WORKED
echo "Criando tabela worked TBL_CLIENTES"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/create-managed-table-clientes-wrk.hql


# Tabela DIVISAO
echo "Carregando Variáveis DIVISAO"
HDFS_DIR="/projeto/desafio_final/dados/DIVISAO"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TBL_DIVISAO_STG"
TARGET_TABLE="TBL_DIVISAO"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Criando a tabela Externa
echo "Criando tabela Externa TBL_DIVISAO_STG"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/create-external-table-divisao-stg.hql

# Criando a tabela WORKED
echo "Criando tabela worked TBL_DIVISAO"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/create-managed-table-divisao-wrk.hql