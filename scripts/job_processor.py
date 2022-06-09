from pyspark.sql import SparkSession, dataframe
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# PEGANDO DADOS DO HIVE
dataframe_alunos = spark.sql("SELECT * FROM desafio_hive.tbl_alunos")

dataframe_alunos_csv.show()



##### EXEMPLOS DE SALVAR ARQUIVOS #####
#SALVAR NO HDFS
dataframe_alunos.write.options(header='True', delimiter=';').csv("/input/dados/dados.csv")

#SALVAR NO DISCO DO LINUX
dataframe_alunos.write.options(header='True', delimiter=';').csv("file:///input/dados/dados.csv")
