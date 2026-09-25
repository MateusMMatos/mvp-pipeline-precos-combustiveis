# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Qualidade depois do pipeline
# MAGIC
# MAGIC Repete nas tabelas finais as verificações feitas na bronze, para mostrar o que o pipeline resolveu e o
# MAGIC que foi mantido por decisão.
# MAGIC
# MAGIC A comparação entre as duas medições atende ao critério de qualidade do enunciado, que pede os
# MAGIC problemas detectados e a forma como foram tratados na modelagem e no pipeline.
# MAGIC
# MAGIC Nem todo problema deve desaparecer. A ausência de complemento no endereço é esperada, e preços
# MAGIC extremos em postos de regiões remotas são informação legítima; removê-los distorceria a análise.

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("combustiveis.silver.precos")
total_silver = silver.count()
print(f"Linhas na silver: {total_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completude: antes e depois
# MAGIC
# MAGIC A comparação usa a tabela de perfil gravada no notebook 02.
# MAGIC
# MAGIC Quatro colunas mudaram de nome entre a bronze e a silver, e o mapeamento abaixo relaciona cada uma ao
# MAGIC nome anterior para alinhar a comparação. Aparecem em apenas um dos lados `valor_compra`, descartada
# MAGIC por estar vazia, e `cnpj_digitos`, criada no pipeline.

# COMMAND ----------

colunas_silver = [c for c in silver.columns if not c.startswith("_")]

expressoes = []
for c in colunas_silver:
    expressoes.append(
        F.count(
            F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == ""), c)
        ).alias(c)
    )

medidas = silver.agg(*expressoes).collect()[0].asDict()

perfil_silver = spark.createDataFrame(
    [(c, int(medidas[c]), round(100.0 * medidas[c] / total_silver, 2)) for c in colunas_silver],
    "coluna string, ausentes_silver bigint, pct_silver double",
)

perfil_silver.write.mode("overwrite").saveAsTable("combustiveis.silver.perfil_completude")

spark.sql(
    "COMMENT ON TABLE combustiveis.silver.perfil_completude IS "
    "'Perfil de completude da camada silver: ausentes e percentual por coluna. Gerado pelo notebook 05 "
    "para comparacao com o perfil da bronze.'"
)
for coluna, descricao in [
    ("coluna", "Nome da coluna da tabela silver.precos analisada. Texto."),
    ("ausentes_silver", "Quantidade de linhas sem valor na coluna, somando nulos e texto vazio. Inteiro."),
    ("pct_silver", "Percentual de linhas sem valor na coluna. Decimal. Dominio: 0 a 100."),
]:
    spark.sql(
        f"COMMENT ON COLUMN combustiveis.silver.perfil_completude.{coluna} IS '{descricao}'"
    )

# Colunas renomeadas no caminho da bronze para a silver
renomeadas = {
    "revenda": "razao_social",
    "cnpj_revenda": "cnpj",
    "nome_rua": "logradouro",
    "numero_rua": "numero",
}
mapa = F.create_map(*[F.lit(x) for par in renomeadas.items() for x in par])

comparacao = (
    spark.table("combustiveis.bronze.perfil_completude")
    .select(
        F.col("coluna").alias("coluna_bronze"),
        F.coalesce(mapa[F.col("coluna")], F.col("coluna")).alias("chave"),
        (F.col("nulos") + F.col("vazios")).alias("ausentes_bronze"),
        F.col("pct_ausente").alias("pct_bronze"),
    )
    .join(perfil_silver, F.col("chave") == F.col("coluna"), "full_outer")
    .select(
        F.coalesce(F.col("coluna"), F.col("coluna_bronze")).alias("coluna_silver"),
        F.col("coluna_bronze"),
        "ausentes_bronze",
        "pct_bronze",
        "ausentes_silver",
        "pct_silver",
    )
    .orderBy(F.desc_nulls_last("pct_bronze"))
)

display(comparacao)

# COMMAND ----------

# MAGIC %md
# MAGIC A ausência de complemento permanece, por ser uma característica do dado, e não um defeito.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistência: antes e depois
# MAGIC
# MAGIC Os mesmos testes de formato do notebook 02, agora sobre os tipos corretos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) AS linhas,
# MAGIC   sum(CASE WHEN valor_venda IS NULL THEN 1 ELSE 0 END) AS precos_invalidos,
# MAGIC   sum(CASE WHEN data_coleta IS NULL THEN 1 ELSE 0 END) AS datas_invalidas,
# MAGIC   sum(CASE WHEN length(cnpj_digitos) <> 14 THEN 1 ELSE 0 END) AS cnpj_invalidos,
# MAGIC   sum(CASE WHEN cnpj <> trim(cnpj) THEN 1 ELSE 0 END) AS cnpj_com_espaco,
# MAGIC   count(DISTINCT unidade_medida) AS unidades_distintas,
# MAGIC   count(DISTINCT numero) FILTER (WHERE numero = 'S/N') AS forma_unica_sem_numero
# MAGIC FROM combustiveis.silver.precos;

# COMMAND ----------

# MAGIC %md
# MAGIC Resultado esperado: zero nas colunas de erro, duas unidades de medida (litro e metro cúbico) e uma
# MAGIC única forma para "sem número". Na bronze, os mesmos testes apontavam 253 preços fora do padrão,
# MAGIC 422.418 CNPJ com espaço, três grafias de unidade e quatro formas de "sem número".

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unicidade

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) AS linhas,
# MAGIC   count(*) - count(DISTINCT cnpj_digitos, produto, data_coleta) AS repeticoes_da_chave
# MAGIC FROM combustiveis.silver.precos;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Integridade do modelo estrela
# MAGIC
# MAGIC Verifica se toda chave da fato encontra a sua dimensão. Uma chave sem correspondência não gera erro na
# MAGIC consulta: as linhas afetadas são descartadas nas junções e o resultado sai menor, sem nenhum aviso.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (SELECT count(*) FROM combustiveis.gold.fato_preco_coleta f
# MAGIC      LEFT JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC     WHERE d.sk_posto IS NULL) AS fato_sem_posto,
# MAGIC   (SELECT count(*) FROM combustiveis.gold.fato_preco_coleta f
# MAGIC      LEFT JOIN combustiveis.gold.dim_produto d ON d.sk_produto = f.sk_produto
# MAGIC     WHERE d.sk_produto IS NULL) AS fato_sem_produto,
# MAGIC   (SELECT count(*) FROM combustiveis.gold.fato_preco_coleta f
# MAGIC      LEFT JOIN combustiveis.gold.dim_bandeira d ON d.sk_bandeira = f.sk_bandeira
# MAGIC     WHERE d.sk_bandeira IS NULL) AS fato_sem_bandeira,
# MAGIC   (SELECT count(*) FROM combustiveis.gold.fato_preco_coleta f
# MAGIC      LEFT JOIN combustiveis.gold.dim_tempo d ON d.data = f.data_coleta
# MAGIC     WHERE d.data IS NULL) AS fato_sem_data;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conservação do volume ao longo do pipeline
# MAGIC
# MAGIC Cada camada deve explicar a diferença de linhas em relação à anterior.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze' AS camada, count(*) AS linhas, 'Dado como publicado pela ANP' AS observacao
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC UNION ALL
# MAGIC SELECT 'silver', count(*), 'Bronze menos as 6 duplicatas exatas'
# MAGIC FROM combustiveis.silver.precos
# MAGIC UNION ALL
# MAGIC SELECT 'gold (fato)', count(*), 'Igual a silver: nenhuma linha se perde nas juncoes'
# MAGIC FROM combustiveis.gold.fato_preco_coleta;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Outliers mantidos
# MAGIC
# MAGIC Os preços extremos continuam na base. A consulta mostra em que estados eles se concentram: se
# MAGIC estivessem em um único posto ou em uma única data, poderiam indicar erro de digitação; distribuídos
# MAGIC por estados com custo logístico alto, indicam diferença regional de preço.
# MAGIC
# MAGIC A ordenação é feita pelo percentual dentro de cada estado, e não pela contagem. São Paulo tem muito
# MAGIC mais postos pesquisados que os estados do Norte e apareceria no topo de qualquer contagem absoluta,
# MAGIC sem que isso significasse preço alto.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH limites AS (
# MAGIC   SELECT p.sk_produto,
# MAGIC          percentile_approx(f.valor_venda, 0.99) AS p99
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   WHERE p.produto = 'GASOLINA'
# MAGIC   GROUP BY p.sk_produto
# MAGIC )
# MAGIC SELECT d.estado_sigla,
# MAGIC        count(*) AS coletas_de_gasolina,
# MAGIC        sum(CASE WHEN f.valor_venda > l.p99 THEN 1 ELSE 0 END) AS acima_do_p99,
# MAGIC        round(100.0 * sum(CASE WHEN f.valor_venda > l.p99 THEN 1 ELSE 0 END) / count(*), 2) AS pct_do_estado,
# MAGIC        round(avg(f.valor_venda), 3) AS preco_medio_do_estado
# MAGIC FROM combustiveis.gold.fato_preco_coleta f
# MAGIC JOIN limites l ON l.sk_produto = f.sk_produto
# MAGIC JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC GROUP BY d.estado_sigla
# MAGIC HAVING sum(CASE WHEN f.valor_venda > l.p99 THEN 1 ELSE 0 END) > 0
# MAGIC ORDER BY pct_do_estado DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo
# MAGIC
# MAGIC | Problema detectado na bronze | Situação nas tabelas finais |
# MAGIC |---|---|
# MAGIC | `valor_compra` inteiramente vazia | Resolvido: coluna descartada, com o motivo registrado no catálogo |
# MAGIC | Preço como texto, em três formatos | Resolvido: decimal(6,3), sem conversões perdidas |
# MAGIC | Data como texto | Resolvido: tipo date |
# MAGIC | CNPJ com espaço à esquerda em um dos arquivos | Resolvido, com chave natural apenas com dígitos |
# MAGIC | Duas grafias para a unidade do GNV | Resolvido: grafia única |
# MAGIC | Quatro formas de "sem número" | Resolvido: forma única S/N |
# MAGIC | 6 linhas duplicadas | Resolvido: removidas |
# MAGIC | Nome de município repetido entre estados | Tratado na modelagem: localidade sempre por estado e município |
# MAGIC | Posto com mais de uma bandeira no período | Tratado na modelagem: bandeira na fato, referente à data da coleta |
# MAGIC | Complemento de endereço ausente | Mantido: característica do dado |
# MAGIC | Preços extremos | Mantidos: diferença regional, com o intervalo documentado |
