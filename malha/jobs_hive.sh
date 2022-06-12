#!/bin/bash

# CLIENTES
echo "efetuando a ingestão de CLIENTES..."
bash ../scripts/update_data_external_table.sh CLIENTES
echo "Ingestão da TBL_CLIENTES_STG concluida"
bash ../scripts/insert_data_worked_table.sh TBL_CLIENTES insert-table-clientes-wrk
echo "Ingestão da TBL_CLIENTES concluida" 

# DIVISAO
echo "efetuando a ingestão de DIVISAO..."
bash ../scripts/update_data_external_table.sh DIVISAO
echo "Ingestão da TBL_DIVISAO_STG concluida"
bash ../scripts/insert_data_worked_table.sh TBL_DIVISAO insert-table-divisao-wrk
echo "Ingestão da TBL_DIVISAO concluida" 

# ENDERECO
echo "efetuando a ingestão de ENDERECO..."
bash ../scripts/update_data_external_table.sh ENDERECO
echo "Ingestão da TBL_ENDERECO_STG concluida"
bash ../scripts/insert_data_worked_table.sh TBL_ENDERECO insert-table-endereco-wrk
echo "Ingestão da TBL_ENDERECO concluida" 

# REGIAO
echo "efetuando a ingestão de REGIAO..."
bash ../scripts/update_data_external_table.sh REGIAO
echo "Ingestão da TBL_REGIAO_STG concluida"
bash ../scripts/insert_data_worked_table.sh TBL_REGIAO insert-table-regiao-wrk
echo "Ingestão da TBL_REGIAO concluida" 

# VENDAS
echo "efetuando a ingestão de VENDAS..."
bash ../scripts/update_data_external_table.sh VENDAS
echo "Ingestão da TBL_VENDAS_STG concluida"
bash ../scripts/insert_data_worked_table.sh TBL_VENDAS insert-table-vendas-wrk
echo "Ingestão da TBL_VENDAS concluida" 