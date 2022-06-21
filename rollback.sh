#!/bin/bash

# Removendo os dados do HDFS
hdfs dfs -rm -r /projeto/desafio_final

# Drop arquivos de tabelas salvos no HDFS
echo "Deletanto tabela de CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES_STG;"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES;"

echo "Deletando tabela de DIVISAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_DIVISAO_STG;"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_DIVISAO;"

echo "Deletando tabela de ENDERECO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_ENDERECO_STG;"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_ENDERECO;"

echo "Deletando tabela de REGIAO"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_REGIAO_STG;"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_REGIAO;"

echo "Deletando tabela de VENDAS"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_VENDAS_STG;"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_VENDAS;"

# Deletando o banco de dados
echo "Deletanto banco do desafio_final"
beeline -u jdbc:hive2://localhost:10000 -e "DROP DATABASE IF EXISTS desafio_final;"