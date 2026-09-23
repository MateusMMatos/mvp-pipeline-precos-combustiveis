# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Camada silver: limpeza e padronizacao
# MAGIC
# MAGIC Le a tabela bronze e grava uma tabela com o dado tipado, padronizado e sem duplicatas. Cada
# MAGIC transformacao aqui corresponde a um problema medido no notebook 02, e o efeito de cada uma e
# MAGIC contado e gravado na tabela `combustiveis.silver.log_transformacoes`.
# MAGIC
# MAGIC O motivo de a camada existir ficou evidente no perfil de qualidade: os dois arquivos vem da mesma
# MAGIC agencia e do mesmo levantamento, mas nao seguem o mesmo padrao. O arquivo de jan-jun/2026 traz o
# MAGIC CNPJ com um espaco a esquerda em todas as linhas e o de jul-dez/2025 nao; o de jul-dez/2025 tem
# MAGIC precos com uma casa decimal e ate sem virgula, o outro nao; a unidade do GNV aparece como
# MAGIC "R$ / m3" em um arquivo e "R$ / m3" com expoente no outro. A bronze preserva essas divergencias
# MAGIC como evidencia; a silver as resolve.
# MAGIC
# MAGIC O que esta camada nao faz: classificar, agrupar ou enriquecer. Isso e modelagem, e acontece na gold.

# COMMAND ----------

from pyspark.sql import functions as F

TABELA_BRONZE = "combustiveis.bronze.precos_anp"
TABELA_SILVER = "combustiveis.silver.precos"
TABELA_LOG = "combustiveis.silver.log_transformacoes"

bronze = spark.table(TABELA_BRONZE)
colunas_origem = [c for c in bronze.columns if not c.startswith("_")]

linhas_bronze = bronze.count()
print(f"Linhas na bronze: {linhas_bronze}")

registro = []


def anotar(transformacao, motivo, linhas_afetadas):
    registro.append((transformacao, motivo, int(linhas_afetadas)))
    print(f"{transformacao}: {linhas_afetadas} linhas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Duplicatas exatas
# MAGIC
# MAGIC Linhas identicas em todas as colunas da fonte. Sao coletas registradas duas vezes: nao acrescentam
# MAGIC informacao e puxariam a media do posto para o valor repetido.

# COMMAND ----------

df = bronze.dropDuplicates(colunas_origem)
linhas_sem_duplicatas = df.count()
anotar(
    "Remocao de duplicatas exatas",
    "Linhas identicas em todas as colunas da fonte, contadas no perfil de qualidade",
    linhas_bronze - linhas_sem_duplicatas,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Espacos no texto
# MAGIC
# MAGIC Remove espaco no inicio e no fim e reduz sequencias de espacos a um so. Sem isso, o mesmo posto
# MAGIC apareceria duas vezes na dimensao por causa de um espaco a mais no nome.

# COMMAND ----------

colunas_texto = [
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
    "unidade_medida",
    "bandeira",
]

condicao_espaco = None
for c in colunas_texto:
    tem_espaco = F.col(c).isNotNull() & (F.col(c) != F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))
    condicao_espaco = tem_espaco if condicao_espaco is None else (condicao_espaco | tem_espaco)

linhas_com_espaco = df.filter(condicao_espaco).count()

for c in colunas_texto:
    df = df.withColumn(c, F.regexp_replace(F.trim(F.col(c)), r"\s+", " "))

anotar(
    "Padronizacao de espacos em campos de texto",
    "Espaco a esquerda no CNPJ em todo o arquivo de 2026.01 e espacos duplos em enderecos",
    linhas_com_espaco,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tipagem do preco
# MAGIC
# MAGIC O preco vem como texto com virgula decimal, e o arquivo de jul-dez/2025 tem tres variantes: duas
# MAGIC casas decimais, uma casa decimal e valores inteiros sem virgula. A conversao trata as tres, e a
# MAGIC contagem de nulos apos a conversao confirma que nenhum valor se perdeu.

# COMMAND ----------

df = df.withColumn(
    "valor_venda_decimal",
    F.regexp_replace(F.col("valor_venda"), ",", ".").cast("decimal(6,3)"),
)

precos_perdidos = df.filter(F.col("valor_venda").isNotNull() & F.col("valor_venda_decimal").isNull()).count()
precos_sem_virgula = df.filter(~F.col("valor_venda").contains(",")).count()

anotar(
    "Conversao do preco para decimal(6,3)",
    "Preco publicado como texto com virgula decimal, incluindo valores com uma casa e sem virgula",
    df.count(),
)
print(f"Precos que nao converteram (esperado 0): {precos_perdidos}")
print(f"Precos publicados sem virgula: {precos_sem_virgula}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tipagem da data
# MAGIC
# MAGIC Texto em dd/mm/aaaa convertido para date. Sem isso, ordenar por data ordenaria alfabeticamente,
# MAGIC colocando 01/12 antes de 02/07.

# COMMAND ----------

df = df.withColumn("data_coleta_date", F.to_date(F.col("data_coleta"), "dd/MM/yyyy"))
datas_perdidas = df.filter(F.col("data_coleta_date").isNull()).count()

anotar(
    "Conversao da data para date",
    "Data publicada como texto em dd/mm/aaaa",
    df.count(),
)
print(f"Datas que nao converteram (esperado 0): {datas_perdidas}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. CNPJ
# MAGIC
# MAGIC Mantem o CNPJ formatado, que e como a ANP publica e como uma pessoa le, e cria uma versao apenas
# MAGIC com digitos para servir de chave natural do posto. Chave com mascara quebra assim que a fonte
# MAGIC mudar a pontuacao.

# COMMAND ----------

df = df.withColumn("cnpj_digitos", F.regexp_replace(F.col("cnpj_revenda"), "[^0-9]", ""))
cnpj_invalido = df.filter(F.length("cnpj_digitos") != 14).count()

anotar(
    "Criacao do CNPJ apenas com digitos",
    "Chave natural do posto, imune a mudanca de mascara na fonte",
    df.count(),
)
print(f"CNPJ sem 14 digitos (esperado 0): {cnpj_invalido}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Unidade de medida
# MAGIC
# MAGIC A unidade do GNV aparece com duas grafias diferentes, uma em cada arquivo. Unificamos na forma sem
# MAGIC caractere especial, para nao depender de codificacao. Enquanto existirem duas grafias, qualquer
# MAGIC agrupamento por unidade separa o GNV em dois grupos que deveriam ser um.

# COMMAND ----------

antes_unidades = df.select("unidade_medida").distinct().count()

df = df.withColumn(
    "unidade_medida",
    F.when(F.col("unidade_medida").rlike("m3|m³"), F.lit("R$ / m3")).otherwise(F.lit("R$ / litro")),
)

depois_unidades = df.select("unidade_medida").distinct().count()
anotar(
    "Unificacao da grafia da unidade de medida",
    f"GNV publicado com duas grafias; unidades distintas passaram de {antes_unidades} para {depois_unidades}",
    df.filter(F.col("unidade_medida") == "R$ / m3").count(),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Numero do logradouro
# MAGIC
# MAGIC As variantes de "sem numero" (S/N, SN, S N, S/No) viram uma forma unica. Valores como 99-A sao
# MAGIC numeros de verdade e ficam como estao.

# COMMAND ----------

numero_normalizado = F.upper(F.regexp_replace(F.col("numero_rua"), r"[^A-Za-z0-9]", ""))
eh_sem_numero = F.col("numero_rua").isNull() | numero_normalizado.rlike("^(SN|SNO|SNUMERO|SEMNUMERO)$")

linhas_sem_numero = df.filter(eh_sem_numero).count()
df = df.withColumn("numero_rua", F.when(eh_sem_numero, F.lit("S/N")).otherwise(F.col("numero_rua")))

anotar(
    "Padronizacao do numero do logradouro",
    "Variantes de sem numero unificadas em S/N",
    linhas_sem_numero,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Descarte da coluna sem conteudo
# MAGIC
# MAGIC `valor_compra` esta vazia em todas as linhas porque a ANP encerrou essa coleta em agosto de 2020,
# MAGIC conforme o dicionario oficial. Manter uma coluna vazia no modelo final convida alguem a tentar
# MAGIC calcular margem com ela.

# COMMAND ----------

anotar(
    "Descarte da coluna valor_compra",
    "Coluna integralmente vazia: coleta descontinuada pela ANP em agosto de 2020",
    linhas_bronze,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Selecao final e gravacao
# MAGIC
# MAGIC Os nomes finais descrevem o conteudo: `revenda` vira `razao_social`, `nome_rua` vira `logradouro`.
# MAGIC Os metadados de controle da bronze sao preservados e um novo marca o processamento da silver.

# COMMAND ----------

silver = df.select(
    F.col("regiao_sigla"),
    F.col("estado_sigla"),
    F.col("municipio"),
    F.col("revenda").alias("razao_social"),
    F.col("cnpj_revenda").alias("cnpj"),
    F.col("cnpj_digitos"),
    F.col("nome_rua").alias("logradouro"),
    F.col("numero_rua").alias("numero"),
    F.col("complemento"),
    F.col("bairro"),
    F.col("cep"),
    F.col("produto"),
    F.col("unidade_medida"),
    F.col("bandeira"),
    F.col("data_coleta_date").alias("data_coleta"),
    F.col("valor_venda_decimal").alias("valor_venda"),
    F.col("_arquivo_origem"),
    F.col("_data_ingestao"),
    F.current_timestamp().alias("_data_processamento_silver"),
)

silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_SILVER)

spark.sql(
    f"COMMENT ON TABLE {TABELA_SILVER} IS "
    "'Precos de combustiveis da ANP tipados, padronizados e sem duplicatas. Uma linha por preco coletado "
    "em um posto. Origem: combustiveis.bronze.precos_anp.'"
)

print(f"Linhas gravadas na silver: {spark.table(TABELA_SILVER).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registro das transformacoes
# MAGIC
# MAGIC A tabela abaixo fica gravada para que a documentacao do pipeline nao dependa de alguem lembrar o
# MAGIC que foi feito.

# COMMAND ----------

log = spark.createDataFrame(
    registro, "transformacao string, motivo string, linhas_afetadas bigint"
).withColumn("_data_execucao", F.current_timestamp())

log.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABELA_LOG)
display(log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validacao da camada
# MAGIC
# MAGIC Tres verificacoes que precisam passar antes de a gold ser construida.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) AS linhas,
# MAGIC   sum(CASE WHEN valor_venda IS NULL THEN 1 ELSE 0 END) AS precos_nulos,
# MAGIC   sum(CASE WHEN data_coleta IS NULL THEN 1 ELSE 0 END) AS datas_nulas,
# MAGIC   sum(CASE WHEN length(cnpj_digitos) <> 14 THEN 1 ELSE 0 END) AS cnpj_invalido,
# MAGIC   count(DISTINCT unidade_medida) AS unidades_distintas,
# MAGIC   min(data_coleta) AS primeira_coleta,
# MAGIC   max(data_coleta) AS ultima_coleta
# MAGIC FROM combustiveis.silver.precos;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT produto, unidade_medida, count(*) AS linhas
# MAGIC FROM combustiveis.silver.precos
# MAGIC GROUP BY produto, unidade_medida
# MAGIC ORDER BY produto;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM combustiveis.silver.precos LIMIT 10;
