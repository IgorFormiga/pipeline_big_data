from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import when, col, sum, count, isnan, round
from pyspark.sql.functions import regexp_replace, concat_ws, sha2, rtrim
from pyspark.sql.functions import unix_timestamp, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql import HiveContext

from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import when

spark = SparkSession.builder.master("local[*]")\
    .enableHiveSupport()\
    .getOrCreate()

# PEGANDO DADOS DO HIVE
df_clientes = spark.sql("SELECT * FROM desafio_final.tbl_clientes")
df_divisao = spark.sql("SELECT * FROM desafio_final.tbl_divisao")
df_endereco = spark.sql("SELECT * FROM desafio_final.tbl_endereco")
df_regiao = spark.sql("SELECT * FROM desafio_final.tbl_regiao")
df_vendas = spark.sql("SELECT * FROM desafio_final.tbl_vendas")


# Definindo funções para tratamento dos dados:

# Função para correção dos dados que são compostos apenas por espaços
def corrigir_dados_formado_por_espaços(df):
    for c in df.columns:
        df = df.withColumn(c,rtrim(df[c]))
        df = df.withColumn(c, when(df[c] == '', 'NÃO INFORMADO')\
                                         .otherwise(df[c]))
    return df

# Função para correção dos dados que são nulos na tabela de acordo com o tipo e a regrea de negocio
def corrigir_dados_null(df):
    for c in df.columns:
        if (str(df.schema[c].dataType) == 'StringType'):
            df = df.withColumn(c, when(df[c].isNull(), 'NÃO INFORMADO').otherwise(df[c]))
        elif (str(df.schema[c].dataType) == 'DoubleType'):
            df = df.withColumn(c, when(df[c].isNull(), 0.0).otherwise(df[c]))
        elif (str(df.schema[c].dataType) == 'IntegerType'):
            df = df.withColumn(c, when(df[c].isNull(), 0).otherwise(df[c]))
    return df


# Função para corrigir as colunas que estão com os tipos errados de dados (não é valido para datas)
def converter_tipo_coluna(df, colunas, tipo):
    if type(colunas) != list:
        colunas = [colunas] 
    for col_name in colunas:
        df = df.withColumn(col_name, col(col_name).cast(tipo))       
    return df


# Tramento da tabela Clientes:
df_clientes = corrigir_dados_formado_por_espaços(df_clientes)
df_clientes = df_clientes.dropDuplicates(['CustomerKey'])

# Tratamento da tabela Endereço:
df_endereco = corrigir_dados_formado_por_espaços(df_endereco)
df_endereco = corrigir_dados_null(df_endereco)

# Corrigindo o valor duplicado "10021911" da colluna Address Number, que assim como na tabela de cliente possui as PK repetidas
df_endereco = df_endereco.dropDuplicates(['Address Number'])


# Tratamento da tabela Vendas:
# Removendo do dataframe todas as linhas que possuem todos os campos nulos
df_vendas = df_vendas.filter(((col("Actual Delivery Date").isNotNull()) & (col("CustomerKey").isNotNull()) & (col("DateKey").isNotNull())))

# Antes de continuar com limpeza dos dados vamos corrigir as colunas que estão com os tipos errados de dados
df_vendas = converter_tipo_coluna(df_vendas, 'Item Number', 'int')

cols_dates = ['Actual Delivery Date', 'DateKey', 'Invoice Date', 'Promised Delivery Date']

for col_name_data in cols_dates:
    df_vendas = df_vendas.withColumn(col_name_data, to_date(col(col_name_data),"dd/MM/yyyy"))

    
# O separador decimal utilizado na base de dados é a virgula, logo, precisamos alteralo para o ponto. E aproveitando o loop 
# vamos relizar o cast
col_lis = ['Discount Amount', 'List Price', 'Sales Amount', 'Sales Amount Based on List Price',\
           'Sales Cost Amount', 'Sales Margin Amount', 'Sales Price']

for coluna in col_lis:
    df_vendas = df_vendas.withColumn(coluna, round(regexp_replace(coluna,'\,','.').cast('double'), 2))

# Realizando a limpeza dos dados que são nulos na tabela
df_vendas = corrigir_dados_null(df_vendas)


# Criando as views:
df_endereco.createOrReplaceTempView('vw_endereco')
df_divisao.createOrReplaceTempView('vw_divisao')
df_clientes.createOrReplaceTempView('vw_clientes')
df_vendas.createOrReplaceTempView('vw_vendas')
df_regiao.createOrReplaceTempView('vw_regiao')


# Criação de um dataframe para servir de base para a dimensão Tempo (time)
df_time_ST = spark.sql('''
                SELECT
                DateKey,
                DAY(DateKey) AS Day, 
                MONTH(DateKey) AS Month,
                DATE_FORMAT(DateKey,"MMMM") AS Month_Name,
                YEAR(DateKey) as Year,
                QUARTER(DateKey) as Quarter
                FROM(
                SELECT 
                EXPLODE(SEQUENCE(TO_DATE('2017-01-09'), TO_DATE('2019-03-18'), INTERVAL 1 DAY)) as DateKey)''')

df_time_ST.createOrReplaceTempView('vw_time_ST')

# Desnormalizando os dados (apenas com as colunas de interesse):

# Junção das tabelas df_cliente, df_divisao e df_regiao
df_clientes_ST = spark.sql('''
            SELECT 
            vw_clientes.CustomerKey,
            vw_clientes.Customer,
            vw_clientes.`Business Family` as Business_Family,
            vw_clientes.`Customer Type` as Customer_Type,
            vw_clientes.`Address Number` as Address_Number,
            vw_divisao.`Division Name` as Division_Name,
            vw_regiao.`Region Name` as Region_Name          
            FROM vw_clientes
            INNER JOIN vw_divisao
            ON vw_clientes.`Division` = vw_divisao.`Division`
            INNER JOIN vw_regiao
            ON vw_clientes.`Region Code` = vw_regiao.`Region Code`''')
df_clientes_ST.createOrReplaceTempView('vw_clientes_ST')


# Junção das tabelas df_clientes_ST e df_endereco
df_clientes_endereco_ST = spark.sql('''
            SELECT 
            vw_clientes_ST.CustomerKey,
            vw_clientes_ST.Customer,
            vw_clientes_ST.Business_Family,
            vw_clientes_ST.Customer_Type,
            vw_clientes_ST.Address_Number,
            vw_clientes_ST.Division_Name,
            vw_clientes_ST.Region_Name, 
            
            
            vw_endereco.City,
            vw_endereco.State,
            vw_endereco.Country
            
            FROM vw_clientes_ST
            LEFT JOIN vw_endereco
            ON vw_clientes_ST.Address_Number = vw_endereco.`Address Number`
''')

# Corrigindo dados nulos, já que nem todos os clientes cadastrados possuem endereço
df_clientes_endereco_ST = corrigir_dados_null(df_clientes_endereco_ST)

df_clientes_endereco_ST.createOrReplaceTempView('vw_clientes_endereco_ST')

# Tabela desnormalizada de todos os dados nécessário para criação do modelo 
df_tabela_desnormalizada_ST = spark.sql('''
            SELECT 
            
            vw_vendas.DateKey,
            vw_vendas.`Discount Amount` as Discount_Amount,
            vw_vendas.`Sales Cost Amount` as Sales_Cost_Amount,
            vw_vendas.`Sales Price` as Sales_Price, 
            vw_vendas.`Sales Quantity` as Sales_Quantity,
            
            
            vw_clientes_endereco_ST.CustomerKey,
            vw_clientes_endereco_ST.Customer,
            vw_clientes_endereco_ST.Business_Family,
            vw_clientes_endereco_ST.Customer_Type,
            
            
            vw_clientes_endereco_ST.Address_Number,
            vw_clientes_endereco_ST.Division_Name,
            vw_clientes_endereco_ST.Region_Name,
            vw_clientes_endereco_ST.City,
            vw_clientes_endereco_ST.State,
            vw_clientes_endereco_ST.Country,
            
            vw_time_ST.Day, 
            vw_time_ST.Month,
            vw_time_ST.Month_Name,
            vw_time_ST.Year,
            vw_time_ST.Quarter
            
            
            FROM vw_vendas
            LEFT JOIN vw_clientes_endereco_ST
            ON vw_vendas.CustomerKey = vw_clientes_endereco_ST.CustomerKey
            LEFT JOIN vw_time_ST
            ON vw_vendas.DateKey = vw_time_ST.DateKey
''')

# Criando as HASH para cada dimensão:
df_tabela_desnormalizada_ST = df_tabela_desnormalizada_ST.withColumn('sk_Customer',
                                                                     sha2(concat_ws('',df_tabela_desnormalizada_ST.CustomerKey, 
                                                                                    df_tabela_desnormalizada_ST.Customer,
                                                                                    df_tabela_desnormalizada_ST.Business_Family,
                                                                                    df_tabela_desnormalizada_ST.Customer_Type),
                                                                                    256))

df_tabela_desnormalizada_ST = df_tabela_desnormalizada_ST.withColumn('sk_Locality',
                                                                     sha2(concat_ws('',df_tabela_desnormalizada_ST.Address_Number, 
                                                                                    df_tabela_desnormalizada_ST.Division_Name,
                                                                                    df_tabela_desnormalizada_ST.City,
                                                                                    df_tabela_desnormalizada_ST.State,
                                                                                    df_tabela_desnormalizada_ST.Country), 
                                                                                    256))

df_tabela_desnormalizada_ST = df_tabela_desnormalizada_ST.withColumn('sk_Time',
                                                                     sha2(concat_ws('',df_tabela_desnormalizada_ST.DateKey,
                                                                                    df_tabela_desnormalizada_ST.Day, 
                                                                                    df_tabela_desnormalizada_ST.Month,
                                                                                    df_tabela_desnormalizada_ST.Month_Name,
                                                                                    df_tabela_desnormalizada_ST.Year,
                                                                                    df_tabela_desnormalizada_ST.Quarter), 
                                                                                    256))

# Criação da view para a tabela desnormalizada:
df_tabela_desnormalizada_ST.createOrReplaceTempView('vw_tabela_desnormalizada_ST')

# Criação dos dataframes para o modelo dimensional estrela:

# Criando as tabelas de dimensões:
DIM_Customer = spark.sql('''
            SELECT 
            DISTINCT sk_Customer,
            CustomerKey,
            Customer,
            Business_Family as `Business Family`,
            Customer_Type as `Customer Type`
            FROM vw_tabela_desnormalizada_ST
''')

DIM_Time = spark.sql('''
            SELECT 
            DISTINCT sk_Time,
            DateKey,
            Day, 
            Month,
            Month_Name as `Month Name`,
            Year,
            Quarter
            FROM vw_tabela_desnormalizada_ST
''')

DIM_Locality = spark.sql('''
            SELECT 
            DISTINCT sk_Locality,
            Address_Number as `Address Number`,
            Division_Name as `Division Name`,
            Region_Name as `Region Name`,
            City,
            State,
            Country
            FROM vw_tabela_desnormalizada_ST
''')

# Criação da tabela fato
fact_orders = spark.sql('''
            SELECT 
            sk_Customer,
            sk_Time,
            sk_Locality,
            Sales_Cost_Amount as `Sales Cost Amount`,
            (Sales_Price * Sales_Quantity) as `Sales Value`,
            ROUND(((Sales_Price * Sales_Quantity) - Sales_Cost_Amount), 2) as `Sales Profit`
            FROM vw_tabela_desnormalizada_ST
''')



#SALVAR NO HDFS
DIM_Customer.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/projeto/desafio_final/dados_saida/DIM_Customer.csv")
DIM_Time.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/projeto/desafio_final/dados_saida/DIM_Time.csv")
DIM_Locality.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/projeto/desafio_final/dados_saida/DIM_Locality.csv")
fact_orders.coalesce(1).write.mode('overwrite').options(header='True', delimiter=';').csv("/projeto/desafio_final/dados_saida/fact_orders.csv")