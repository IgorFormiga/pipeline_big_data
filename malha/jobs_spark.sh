#!/bin/bash
# Este shell deve ser executado dentro  do SPARK SERVER

spark-submit \
    --master local[*] \
    --deploy-mode client \
    job_processor.py


# Movendo os arquivos do HDFS para a pasta input/desafio_bigdata_fianl_indra
DIR_HDFS="/projeto/desafio_final/dados_saida"
DIR_UNIX="/input/desafio_bigdata_final_indra/dados/dados_saida"

# DIM_Customer
hdfs dfs -get ${DIR_HDFS}/DIM_Customer.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/DIM_Customer.csv
# DIM_Locality
hdfs dfs -get ${DIR_HDFS}/DIM_Locality.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/DIM_Locality.csv
# DIM_Time
hdfs dfs -get ${DIR_HDFS}/DIM_Time.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/DIM_Time.csv
# fact_orders
hdfs dfs -get ${DIR_HDFS}/fact_orders.csv/*.csv ${DIR_UNIX}
mv ${DIR_UNIX}/part* ${DIR_UNIX}/fact_orders.csv