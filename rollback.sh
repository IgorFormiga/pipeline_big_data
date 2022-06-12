#!/bin/bash

# Removendo os dados do HDFS
hdfs dfs -rm -r /projeto/desafio_final

# Drop arquivos de tabelas salvos no HDFS
echo "Deletanto tabela de ALUNOS"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES_STG;"
beeline -u jdbc:hive2://localhost:10000 -e "DROP TABLE IF EXISTS desafio_final.TBL_CLIENTES;"



# Deletando o banco de dados
echo "Deletanto banco do desafio_final"
beeline -u jdbc:hive2://localhost:10000 -e "DROP DATABASE IF EXISTS desafio_final;"