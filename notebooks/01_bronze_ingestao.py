# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Camada bronze: ingestao dos arquivos da ANP
# MAGIC
# MAGIC Le os dois arquivos CSV que estao no volume e grava uma tabela Delta com o conteudo exatamente
# MAGIC como veio da fonte. Nenhum valor e corrigido, convertido ou descartado nesta etapa.
# MAGIC
# MAGIC Duas decisoes definem a camada bronze:
# MAGIC
# MAGIC 1. Todas as colunas sao lidas como texto. O arquivo traz preco com virgula decimal e data em
# MAGIC    dd/mm/aaaa; deixar o Spark adivinhar o tipo aqui faria a conversao acontecer sem registro e
# MAGIC    descartaria em silencio o que nao coubesse no tipo escolhido. A tipagem e trabalho da camada
# MAGIC    silver, onde fica documentada.
# MAGIC 2. Cada linha recebe dois metadados de controle: o arquivo de origem e o momento da ingestao.
# MAGIC    Sem isso, depois de unir os dois semestres nao ha como saber de qual arquivo veio um registro,
# MAGIC    nem quando ele entrou.
# MAGIC
# MAGIC Os nomes das colunas sao padronizados (sem espacos, acentos ou hifens) porque o cabecalho original
# MAGIC traz nomes como "Regiao - Sigla" e um marcador BOM no inicio do arquivo, que nao servem como nome
# MAGIC de coluna em tabela. Os valores permanecem intactos.

# COMMAND ----------

from pyspark.sql import functions as F

VOLUME = "/Volumes/combustiveis/bronze/arquivos_anp"
TABELA_BRONZE = "combustiveis.bronze.precos_anp"

COLUNAS = [
    "regiao_sigla",
    "estado_sigla",
    "municipio",
    "revenda",
    "cnpj_revenda",
    "nome_rua",
    "numero_rua",
    "complemento",
    "bairro",
    "cep",
    "produto",
    "data_coleta",
    "valor_venda",
    "valor_compra",
    "unidade_medida",
    "bandeira",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Arquivos encontrados no volume

# COMMAND ----------

arquivos = [a.path for a in dbutils.fs.ls(VOLUME) if a.name.endswith(".csv")]
for a in arquivos:
    print(a)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura
# MAGIC
# MAGIC Cada arquivo e lido separadamente para que o nome da origem seja gravado em cada linha, e os
# MAGIC resultados sao empilhados. Os dois semestres tem o mesmo cabecalho, conferido na etapa de busca.

# COMMAND ----------

def ler_csv_anp(caminho):
    df = (
        spark.read.format("csv")
        .option("sep", ";")
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .load(caminho)
    )
    df = df.toDF(*COLUNAS)
    return df.withColumn("_arquivo_origem", F.lit(caminho.split("/")[-1])).withColumn(
        "_data_ingestao", F.current_timestamp()
    )


bronze = None
for caminho in arquivos:
    parte = ler_csv_anp(caminho)
    print(caminho.split("/")[-1], parte.count(), "linhas")
    bronze = parte if bronze is None else bronze.unionByName(parte)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravacao da tabela Delta
# MAGIC
# MAGIC A escrita usa overwrite para que o notebook possa ser reexecutado do zero sem duplicar dados.

# COMMAND ----------

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_BRONZE)
)

spark.sql(
    f"COMMENT ON TABLE {TABELA_BRONZE} IS "
    "'Precos de combustiveis da ANP como publicados, sem tratamento. Uma linha por preco coletado em um posto. "
    "Fonte: ANP, Serie Historica de Precos de Combustiveis, arquivos semestrais de jul-dez/2025 e jan-jun/2026.'"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferencia da carga
# MAGIC
# MAGIC A contagem por arquivo tem que bater com a contagem feita nos CSVs antes do upload:
# MAGIC 384.208 linhas no arquivo de 2025.02 e 422.418 no de 2026.01, totalizando 806.626.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT _arquivo_origem, count(*) AS linhas
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY _arquivo_origem
# MAGIC ORDER BY _arquivo_origem;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS total_linhas FROM combustiveis.bronze.precos_anp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM combustiveis.bronze.precos_anp LIMIT 10;
