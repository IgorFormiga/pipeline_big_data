#!/bin/bash

# Criar pastas do HDFS para armazenar os dados:
echo "Criando pasta desafio_final no HDFS"
hdfs dfs -mkdir -p /projeto/desafio_final/dados

# Enviando todos os arquivos para o HDFS
# echo "Movendo arquivos para o HDFS"
# hdfs dfs -put /input/desafio_bigdata_final_indra/dados/dados_entrada/* /projeto/desafio_final/dados/

# Criação do Banco de Dados
# echo "Criação do Banco de dados"
# beeline -u jdbc:hive2://localhost:10000 -e "CREATE DATABASE IF NOT EXISTS desafio_final;"

# Executar o Create Tables
echo "Executar a criação das Tabelas"
cd scripts/create_tables
bash create_tables.sh
echo "Criação concluida"
