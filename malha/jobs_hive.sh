#!/bin/bash

echo "efetuando a ingestão de CLIENTES..."
bash ../scripts/update_data_external_table.sh CLIENTES
echo "Ingestão da TBL_CLIENTES_STG concluida"
bash ../scripts/insert_data_worked_table.sh TBL_CLIENTES insert-table-clientes-wrk
echo "Ingestão da TBL_CLIENTES concluida" 

echo "efetuando a ingestão de DIVISAO..."
bash ../scripts/update_data_external_table.sh DIVISAO
echo "Ingestão da TBL_CLIENTES_STG concluida"
bash ../scripts/insert_data_worked_table.sh TBL_DIVISAO insert-table-divisao-wrk
echo "Ingestão da TBL_CLIENTES concluida" 



# verificar a partição
#echo "Listando as Partições TBL_CLIENTES_STG"
#beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_CLIENTES_STG;"

# descrever a tabela
echo "Descrevendo a Tabela TBL_CLIENTES_STG"
beeline -u jdbc:hive2://localhost:10000 -e "DESC FORMATTED desafio_final.TBL_CLIENTES_STG;"

# count na tabela
echo "Quantidade de Registros da Tabela TBL_CLIENTES_STG"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_CLIENTES_STG;"

# mostrar as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros linhas TBL_CLIENTES_STG"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_CLIENTES_STG LIMIT 10;"

# ----------------------

# verificar a partição
echo "Listando as Partições TBL_CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "SHOW PARTITIONS desafio_final.TBL_CLIENTES;"

# descrever a tabela
echo "Descrevendo a Tabela TBL_CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "DESC FORMATTED desafio_final.TBL_CLIENTES;"

# count na tabela
echo "Quantidade de Registros da Tabela TBL_CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT COUNT(*) FROM desafio_final.TBL_CLIENTES;"

# mostrar as 10 primeiras linhas da tabela
echo "Mostrando Apenas os 10 primeiros linhas TBL_CLIENTES"
beeline -u jdbc:hive2://localhost:10000 -e "SELECT * FROM desafio_final.TBL_CLIENTES LIMIT 10;"
