
HDFS_DIR="/projeto/desafio_final/dados/"
NOME_ARQUIVO=$1


echo "Efetuando a ingestão na tabela CLIENTE_STG"
cd ../dados/dados_entrada
hdfs dfs -put -f ${NOME_ARQUIVO}.csv ${HDFS_DIR}/${NOME_ARQUIVO}
# hdfs dfs -put -f ${NOME_ARQUIVO}.csv ${HDFS_DIR}/${NOME_ARQUIVO}