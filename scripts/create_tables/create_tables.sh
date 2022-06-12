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


# DIVISAO
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


 # ENDERECO
echo "Carregando Variáveis ENDERECO"
HDFS_DIR="/projeto/desafio_final/dados/ENDERECO"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TBL_ENDERECO_STG"
TARGET_TABLE="TBL_ENDERECO"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Criando a tabela Externa
echo "Criando tabela Externa TBL_ENDERECO_STG"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/create-external-table-endereco-stg.hql

# Criando a tabela WORKED
echo "Criando tabela worked TBL_ENDERECO"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/create-managed-table-endereco-wrk.hql


# REGIAO
echo "Carregando Variáveis REGIAO"
HDFS_DIR="/projeto/desafio_final/dados/REGIAO"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TBL_REGIAO_STG"
TARGET_TABLE="TBL_REGIAO"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Criando a tabela Externa
echo "Criando tabela Externa TBL_REGIAO_STG"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/create-external-table-regiao-stg.hql

# Criando a tabela WORKED
echo "Criando tabela worked TBL_REGIAO"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/create-managed-table-regiao-wrk.hql


 # VENDAS
echo "Carregando Variáveis VENDAS"
HDFS_DIR="/projeto/desafio_final/dados/VENDAS"
TARGET_DATABASE="desafio_final"
TARGET_TABLE_EXTERNAL="TBL_VENDAS_STG"
TARGET_TABLE="TBL_VENDAS"
DT_FOTO="$(date --date="-1 day" "+%Y-%m-%d")"


# Criando a tabela Externa
echo "Criando tabela Externa TBL_VENDAS_STG"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar HDFS_DIR="${HDFS_DIR}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE_EXTERNAL}"\
 -f ../../hqls/create-external-table-vendas-stg.hql

# Criando a tabela WORKED
echo "Criando tabela worked TBL_VENDAS"
beeline -u jdbc:hive2://localhost:10000 \
 --hivevar TARGET_DATABASE="${TARGET_DATABASE}"\
 --hivevar TARGET_TABLE="${TARGET_TABLE}" \
 -f ../../hqls/create-managed-table-vendas-wrk.hql