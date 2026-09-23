# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Camada gold: modelo estrela
# MAGIC
# MAGIC Constroi as quatro dimensoes e a tabela de fatos desenhadas em `docs/modelagem.md`, a partir da
# MAGIC camada silver.
# MAGIC
# MAGIC Grao da fato: um preco, de um combustivel, em um posto, em uma data de coleta. E o nivel mais fino
# MAGIC que a fonte fornece, e guardar nele mantem todas as agregacoes possiveis (semana, mes, municipio,
# MAGIC estado, bandeira).
# MAGIC
# MAGIC Cada dimensao recebe uma chave substituta, numerica e sequencial. A chave natural continua gravada
# MAGIC na dimensao para rastreabilidade.

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
# MAGIC Alem do nome publicado pela ANP, a dimensao classifica o combustivel em grupo e marca se ele pode
# MAGIC entrar em comparacoes por litro. O GNV e vendido em metro cubico: incluir o preco dele em uma media
# MAGIC por estado produziria um numero sem significado. A marcacao deixa essa regra explicita no modelo,
# MAGIC em vez de depender de quem escreve a consulta lembrar dela.

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
# MAGIC A classificacao em BANDEIRADO e BRANCA vem da definicao da propria ANP: posto bandeirado exibe a
# MAGIC marca de uma distribuidora e so vende o combustivel dela; posto de bandeira branca nao exibe marca.
# MAGIC E essa coluna que responde a pergunta P4 sem precisar listar as dezenas de marcas existentes.

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
# MAGIC Um registro por posto, identificado pelo CNPJ apenas com digitos.
# MAGIC
# MAGIC Os atributos cadastrais de um mesmo posto podem variar entre coletas, porque a fonte atualiza o
# MAGIC cadastro ao longo do tempo. A regra adotada e guardar a versao mais recente observada, o que em
# MAGIC modelagem dimensional e a dimensao de mudanca lenta do tipo 1: o valor antigo e sobrescrito e nao
# MAGIC ha historico do atributo. A escolha cabe aqui porque nenhuma das cinco perguntas depende do
# MAGIC endereco antigo de um posto. A bandeira, que muda e importa para a analise, nao entra nesta
# MAGIC dimensao justamente por isso.

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
# MAGIC Gerada como calendario continuo entre a primeira e a ultima coleta, e nao apenas com as datas
# MAGIC presentes nos dados. Assim, uma consulta por mes mostra um mes sem coleta como zero, em vez de
# MAGIC simplesmente omitir a linha e dar a impressao de que o mes nao existiu.
# MAGIC
# MAGIC A semana e identificada pela data da segunda-feira que a inicia (`semana_inicio`), e nao por um
# MAGIC rotulo de ano e numero da semana. O rotulo textual tem um problema conhecido na virada do ano: a
# MAGIC semana de 29/12/2025 a 04/01/2026 pertence a dois anos civis, e qualquer convencao de nome gera
# MAGIC ambiguidade. Uma data nunca e ambigua, ordena corretamente e serve de chave de agrupamento na
# MAGIC pergunta P5. O rotulo `ano_semana` continua disponivel apenas para leitura.

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
# MAGIC Junta a silver com as tres dimensoes para trocar os atributos descritivos pelas chaves substitutas.
# MAGIC A data fica como chave da dimensao de tempo.
# MAGIC
# MAGIC As juncoes sao feitas por: produto e unidade de medida com dim_produto; bandeira com dim_bandeira;
# MAGIC CNPJ apenas com digitos com dim_posto. Como as tres dimensoes foram derivadas da propria silver,
# MAGIC nenhuma linha pode ficar sem correspondencia, e a validacao logo abaixo confirma isso.

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
# MAGIC ## Validacao do modelo
# MAGIC
# MAGIC Duas perguntas que precisam ser respondidas antes de usar o modelo em qualquer analise:
# MAGIC a fato preservou todas as linhas da silver, e nenhuma chave ficou sem correspondencia.

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
# MAGIC Teste de uso do modelo: a consulta abaixo usa a fato e tres dimensoes ao mesmo tempo. Se o modelo
# MAGIC estiver correto, ela responde a pergunta P1 em poucas linhas de SQL.

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
