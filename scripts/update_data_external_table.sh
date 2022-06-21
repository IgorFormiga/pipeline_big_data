
HDFS_DIR="/projeto/desafio_final/dados/"
NOME_ARQUIVO=$1


cd ../dados/dados_entrada
hdfs dfs -put -f ${NOME_ARQUIVO}.csv ${HDFS_DIR}/${NOME_ARQUIVO}
