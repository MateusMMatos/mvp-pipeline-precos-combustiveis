# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Camada gold: modelo estrela
# MAGIC
# MAGIC Constrói as quatro dimensões e a tabela de fatos definidas em `docs/modelagem.md`, a partir da camada
# MAGIC silver.
# MAGIC
# MAGIC Grão da fato: um preço, de um combustível, em um posto, em uma data de coleta. É o nível mais
# MAGIC detalhado que a fonte oferece, o que mantém possíveis todas as agregações usadas na análise (semana,
# MAGIC mês, município, estado e bandeira).
# MAGIC
# MAGIC Cada dimensão recebe uma chave substituta numérica e sequencial. A chave natural é mantida na dimensão
# MAGIC para rastreabilidade.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

TABELA_SILVER = "combustiveis.silver.precos"

silver = spark.table(TABELA_SILVER)
print(f"Linhas na silver: {silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_produto
# MAGIC
# MAGIC Além do nome publicado pela ANP, a dimensão classifica o combustível em grupos e indica se ele pode
# MAGIC entrar em comparações por litro. Como o GNV é vendido em metro cúbico, incluí-lo em uma média por
# MAGIC estado produziria um valor sem significado. A indicação no modelo evita que essa regra dependa de quem
# MAGIC escreve cada consulta.

# COMMAND ----------

dim_produto = (
    silver.select("produto", "unidade_medida")
    .distinct()
    .withColumn(
        "grupo_combustivel",
        F.when(F.col("produto").startswith("GASOLINA"), F.lit("GASOLINA"))
        .when(F.col("produto") == "ETANOL", F.lit("ETANOL"))
        .when(F.col("produto").startswith("DIESEL"), F.lit("DIESEL"))
        .otherwise(F.lit("GNV")),
    )
    .withColumn("comparavel_por_litro", F.col("unidade_medida") == F.lit("R$ / litro"))
    .withColumn("sk_produto", F.row_number().over(Window.orderBy("produto", "unidade_medida")))
    .select("sk_produto", "produto", "unidade_medida", "grupo_combustivel", "comparavel_por_litro")
)

dim_produto.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "combustiveis.gold.dim_produto"
)
display(spark.table("combustiveis.gold.dim_produto"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_bandeira
# MAGIC
# MAGIC A classificação em BANDEIRADO e BRANCA segue a definição da ANP: o posto bandeirado exibe a marca de
# MAGIC uma distribuidora e só vende o combustível dela; o posto de bandeira branca não exibe marca. Essa
# MAGIC coluna permite responder à P4 sem listar as dezenas de marcas existentes.

# COMMAND ----------

dim_bandeira = (
    silver.select("bandeira")
    .distinct()
    .withColumn(
        "tipo_bandeira",
        F.when(F.col("bandeira") == "BRANCA", F.lit("BRANCA")).otherwise(F.lit("BANDEIRADO")),
    )
    .withColumn("sk_bandeira", F.row_number().over(Window.orderBy("bandeira")))
    .select("sk_bandeira", "bandeira", "tipo_bandeira")
)

dim_bandeira.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "combustiveis.gold.dim_bandeira"
)
print(f"Bandeiras: {dim_bandeira.count()}")
display(spark.table("combustiveis.gold.dim_bandeira").orderBy("bandeira"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_posto
# MAGIC
# MAGIC Um registro por posto, identificado pelo CNPJ apenas com dígitos.
# MAGIC
# MAGIC Os atributos cadastrais de um posto podem variar entre coletas, porque a fonte atualiza o cadastro ao
# MAGIC longo do tempo. A regra adotada é guardar a versão mais recente, o que corresponde a uma dimensão de
# MAGIC mudança lenta do tipo 1: o valor anterior é sobrescrito e o histórico do atributo não é mantido. A
# MAGIC escolha é adequada porque nenhuma das perguntas depende do endereço anterior de um posto. A bandeira,
# MAGIC cujo histórico importa para a análise, fica fora desta dimensão por esse motivo.

# COMMAND ----------

janela_posto = Window.partitionBy("cnpj_digitos").orderBy(F.col("data_coleta").desc())

dim_posto = (
    silver.withColumn("ordem", F.row_number().over(janela_posto))
    .filter(F.col("ordem") == 1)
    .select(
        "cnpj_digitos",
        "cnpj",
        "razao_social",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "municipio",
        "estado_sigla",
        "regiao_sigla",
    )
    .withColumn("sk_posto", F.row_number().over(Window.orderBy("cnpj_digitos")))
    .select(
        "sk_posto",
        "cnpj_digitos",
        "cnpj",
        "razao_social",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "municipio",
        "estado_sigla",
        "regiao_sigla",
    )
)

dim_posto.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "combustiveis.gold.dim_posto"
)
print(f"Postos: {spark.table('combustiveis.gold.dim_posto').count()}")
display(spark.table("combustiveis.gold.dim_posto").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_tempo
# MAGIC
# MAGIC Gerada como um calendário contínuo entre a primeira e a última coleta, e não apenas com as datas
# MAGIC presentes nos dados. Assim, um mês sem coletas aparece nas consultas com valor zero, em vez de
# MAGIC simplesmente não aparecer.
# MAGIC
# MAGIC A semana é identificada pela data da segunda-feira em que começa (`semana_inicio`), e não por um
# MAGIC rótulo de ano e número da semana. Na virada do ano, a semana de 29/12/2025 a 04/01/2026 pertence a
# MAGIC dois anos civis, e o rótulo fica ambíguo. A data não tem esse problema, mantém a ordenação correta e é
# MAGIC usada como chave de agrupamento na P5. O rótulo `ano_semana` fica disponível apenas para leitura.

# COMMAND ----------

limites = silver.agg(
    F.min("data_coleta").alias("inicio"), F.max("data_coleta").alias("fim")
).collect()[0]

dim_tempo = (
    spark.sql(
        f"SELECT explode(sequence(DATE'{limites['inicio']}', DATE'{limites['fim']}', INTERVAL 1 DAY)) AS data"
    )
    .withColumn("ano", F.year("data"))
    .withColumn("mes", F.month("data"))
    .withColumn("dia", F.dayofmonth("data"))
    .withColumn("ano_mes", F.date_format("data", "yyyy-MM"))
    .withColumn("semana_inicio", F.date_sub(F.col("data"), F.expr("(dayofweek(data) + 5) % 7")))
    .withColumn(
        "ano_semana",
        F.concat(
            F.year("semana_inicio"),
            F.lit("-S"),
            F.lpad(F.weekofyear("semana_inicio").cast("string"), 2, "0"),
        ),
    )
    .withColumn("trimestre", F.quarter("data"))
    .withColumn("semestre", F.when(F.month("data") <= 6, F.lit(1)).otherwise(F.lit(2)))
    .withColumn("dia_semana", F.date_format("data", "EEEE"))
)

dim_tempo.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "combustiveis.gold.dim_tempo"
)
print(f"Dias no calendario: {dim_tempo.count()} (de {limites['inicio']} a {limites['fim']})")
display(spark.table("combustiveis.gold.dim_tempo").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## fato_preco_coleta
# MAGIC
# MAGIC Une a silver às três dimensões para substituir os atributos descritivos pelas chaves substitutas. A
# MAGIC data permanece como chave da dimensão de tempo.
# MAGIC
# MAGIC As junções usam produto e unidade de medida com a dim_produto, bandeira com a dim_bandeira e CNPJ
# MAGIC apenas com dígitos com a dim_posto. Como as dimensões foram derivadas da própria silver, todas as
# MAGIC linhas devem encontrar correspondência, o que é conferido na validação a seguir.

# COMMAND ----------

fato = (
    silver.join(
        spark.table("combustiveis.gold.dim_produto").select("sk_produto", "produto", "unidade_medida"),
        on=["produto", "unidade_medida"],
        how="inner",
    )
    .join(
        spark.table("combustiveis.gold.dim_bandeira").select("sk_bandeira", "bandeira"),
        on=["bandeira"],
        how="inner",
    )
    .join(
        spark.table("combustiveis.gold.dim_posto").select("sk_posto", "cnpj_digitos"),
        on=["cnpj_digitos"],
        how="inner",
    )
    .select(
        "sk_posto",
        "sk_produto",
        "sk_bandeira",
        F.col("data_coleta"),
        F.col("valor_venda"),
    )
)

fato.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "combustiveis.gold.fato_preco_coleta"
)

spark.sql(
    "COMMENT ON TABLE combustiveis.gold.fato_preco_coleta IS "
    "'Uma linha por preco de um combustivel coletado em um posto em uma data. Medida: valor_venda. "
    "Origem: combustiveis.silver.precos.'"
)

print(f"Linhas na fato: {spark.table('combustiveis.gold.fato_preco_coleta').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validação do modelo
# MAGIC
# MAGIC Duas verificações antes de usar o modelo na análise: se a fato manteve todas as linhas da silver e se
# MAGIC alguma chave ficou sem correspondência.

# COMMAND ----------

linhas_silver = silver.count()
linhas_fato = spark.table("combustiveis.gold.fato_preco_coleta").count()

print(f"Linhas na silver: {linhas_silver}")
print(f"Linhas na fato:   {linhas_fato}")
print(f"Diferenca (esperado 0): {linhas_silver - linhas_fato}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   sum(CASE WHEN sk_posto IS NULL THEN 1 ELSE 0 END) AS sem_posto,
# MAGIC   sum(CASE WHEN sk_produto IS NULL THEN 1 ELSE 0 END) AS sem_produto,
# MAGIC   sum(CASE WHEN sk_bandeira IS NULL THEN 1 ELSE 0 END) AS sem_bandeira,
# MAGIC   sum(CASE WHEN data_coleta IS NULL THEN 1 ELSE 0 END) AS sem_data,
# MAGIC   sum(CASE WHEN valor_venda IS NULL THEN 1 ELSE 0 END) AS sem_preco
# MAGIC FROM combustiveis.gold.fato_preco_coleta;

# COMMAND ----------

# MAGIC %md
# MAGIC Teste de uso do modelo: a consulta a seguir combina a fato com duas dimensões e responde à P1 com
# MAGIC poucas linhas de SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.ano_mes,
# MAGIC        p.produto,
# MAGIC        round(avg(f.valor_venda), 3) AS preco_medio,
# MAGIC        count(*) AS coletas
# MAGIC FROM combustiveis.gold.fato_preco_coleta f
# MAGIC JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC JOIN combustiveis.gold.dim_tempo t ON t.data = f.data_coleta
# MAGIC WHERE p.produto = 'GASOLINA'
# MAGIC GROUP BY t.ano_mes, p.produto
# MAGIC ORDER BY t.ano_mes;
