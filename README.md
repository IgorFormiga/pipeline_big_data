# DESAFIO BIG DATA - TURMA UNIESP

## ESCOPO DO DESAFIO
- você esta iniciando sua jornada no mundo da engenharia de dados, tivemos algumas aulas de muito aprendizado, pois bem chegou a hora de colocar todo aprendizado em pratica.
- neste desafio você ira efetuar as ingestões dos dados que estão na pasta dados/dados_entrada onde vamos ter alguns csv de um banco de vendas
       - VENDAS.CSV
       - CLIENTES.CSV
       - ENDERECO.CSV
       - REGIAO.CSV
       - DIVISAO.CSV
- seu trabalho como engenheiro de dados é prover dados em uma pasta dados/dados_saida em csv para ser consumido por um relatorio em PowerBI que vocês deverão contruir que esta na pasta app.

# ETAPAS

       - ENVIAR OS ARQUIVOS PARA O HDFS
       - CRIAR TABELAS NO HIVE
              - TBL_VENDAS
              - TBL_VENDAS_STG
              - TBL_CLIENTES
              - TBL_CLIENTES_STG
              - TBL_ENDERECO
              - TBL_ENDERECO_STG
              - TBL_REGIAO
              - TBL_REGIAO_STG
              - TBL_DIVISAO
              - TBL_DIVISAO_STG
       - PROCESSAR OS DADOS NO SPARK EFETUANDO SUAS DEVIDAS TRANSFORMAÇÕES
       - GRAVAR AS INFORMAÇÕES EM TABELAS
              - FT_VENDAS
              - DIM_CLIENTES
              - DIM_TEMPO
              - DIM_LOCALIDADE
       - EXPORTAR OS DADOS PARA A PASTA dados/dados_saida
       - EDITAR O POWERBI COM OS DADOS QUE VOCÊ TRABALHOU 
       - CRIAR GRAFICOS DE VENDAS
       - CRIAR UMA DOCUMENTAÇÃO COM OS TESTES E ETAPAS DO PROJETO

# REGRAS

       - CAMPOS STRINGS VAZIAS DEVERÃO SER PREENCIDAS COM 'NÃO INFORMADO'
       - CAMPOS DECIMAL VAZIOS OU NULOS DEVERÃO SER PREENCHIDOS POR 0
       - SE ATENTEM A MODELAGEM DE DADOS,  FATO (FT_VENDAS) E DIMENSÕES (DIM_CLIENTE, DIM_TEMPO, DIM_LOCALIDADE), ESTE PONTO NÃO SERÁ AVALIADO, MAS SERÁ DIFERENCIAL 
       - NA TABELA FATO PELO MENOS A METRICA VALOR DE VENDA É UM REQUISITO OBRIGATORIO
       - NAS DIMENSÕES DEVERÁ CONTER VALORES UNICOS (DISTINCT), NÃO DEVERÁ CONTER VALORES REPETIDOS      

# INSTRUÇÕES
       - CRIAR OS SCRIPTS COMO FEITO EM SALA DE AULA, USANDO OS PADRÕES E AS PASTAS DEFINIDAS
       - PARA EXECUTAR A MALHA DO HIVE ENTRAR NO HIVER-SERVER (DOCKER.EXE EXEC -IT HIVE-SERVER BASH)
       - PARA EXECUTAR O PROCESSAMENTO ENTRAR NO SPARK (DOCKER.EXE EXEC -IT HIVE-SERVER BASH)
