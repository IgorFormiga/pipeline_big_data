#!/bin/bash

# Criar pastas do HDFS para armazenar os dados:
echo "Criando pasta desafio_final no HDFS"
hdfs dfs -mkdir -p /projeto/desafio_final/dados
hdfs dfs -mkdir -p /projeto/desafio_final/dados/CLIENTES
hdfs dfs -mkdir -p /projeto/desafio_final/dados/DIVISAO

# Executar o Create Tables
echo "Executar a criação das Tabelas"
cd scripts/create_tables
bash create_tables.sh
echo "Criação concluida"
