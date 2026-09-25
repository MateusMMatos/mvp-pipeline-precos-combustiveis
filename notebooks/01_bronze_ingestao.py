# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Camada bronze: ingestão dos arquivos da ANP
# MAGIC
# MAGIC Lê os dois arquivos CSV do volume e grava uma tabela Delta com o conteúdo como veio da fonte, sem
# MAGIC correção, conversão ou descarte de valores.
# MAGIC
# MAGIC Duas decisões definem esta camada:
# MAGIC
# MAGIC 1. Todas as colunas são lidas como texto. O arquivo traz preços com vírgula decimal e datas em
# MAGIC    dd/mm/aaaa, e a inferência automática de tipos do Spark poderia converter ou descartar valores sem
# MAGIC    deixar registro. A tipagem é feita na camada silver, onde fica documentada.
# MAGIC 2. Cada linha recebe dois metadados de controle: o nome do arquivo de origem e o momento da ingestão.
# MAGIC    Sem eles, depois de unir os dois semestres, não seria possível saber de qual arquivo veio cada
# MAGIC    registro.
# MAGIC
# MAGIC Os nomes das colunas são padronizados (sem espaços, acentos ou hífens), porque o cabeçalho original
# MAGIC usa nomes como "Regiao - Sigla" e tem um marcador BOM no início do arquivo. Os valores não são
# MAGIC alterados.

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
# MAGIC Cada arquivo é lido separadamente, para que o nome da origem seja gravado em suas linhas, e os
# MAGIC resultados são unidos em seguida. Os dois semestres têm o mesmo cabeçalho, verificado na etapa de
# MAGIC busca dos dados.

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
# MAGIC ## Gravação da tabela Delta
# MAGIC
# MAGIC A gravação usa o modo overwrite, para que o notebook possa ser reexecutado sem duplicar dados.

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
# MAGIC ## Conferência da carga
# MAGIC
# MAGIC A contagem por arquivo deve coincidir com a contagem feita nos CSV antes do upload: 384.208 linhas no
# MAGIC arquivo de 2025.02 e 422.418 no de 2026.01, com total de 806.626.

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
