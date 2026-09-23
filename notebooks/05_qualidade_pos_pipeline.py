# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Qualidade depois do pipeline
# MAGIC
# MAGIC Repete nas tabelas finais as mesmas verificacoes feitas na bronze, para mostrar o que o pipeline
# MAGIC resolveu e o que permanece por decisao consciente.
# MAGIC
# MAGIC Medir so no inicio prova que havia problema; medir so no fim prova que esta limpo. A comparacao
# MAGIC entre os dois momentos e o que demonstra o efeito do pipeline, e e ela que responde ao criterio de
# MAGIC qualidade do enunciado: problemas detectados e como foram considerados na modelagem e no pipeline.
# MAGIC
# MAGIC Nem todo problema deve desaparecer. Ausencia de complemento em endereco e normal, e preco extremo
# MAGIC em posto de rodovia no interior e informacao legitima. Apagar isso deixaria a base bonita e a
# MAGIC analise errada.

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("combustiveis.silver.precos")
total_silver = silver.count()
print(f"Linhas na silver: {total_silver}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completude: antes e depois
# MAGIC
# MAGIC A comparacao usa a tabela de perfil gravada na etapa de qualidade da bronze.
# MAGIC
# MAGIC Quatro colunas mudaram de nome no caminho, e o mapeamento abaixo liga cada uma ao nome antigo,
# MAGIC para que a comparacao fique alinhada em vez de mostrar dois lados soltos. As unicas colunas que
# MAGIC aparecem em um lado so sao `valor_compra`, descartada por estar vazia, e `cnpj_digitos`, criada
# MAGIC pelo pipeline.

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
# MAGIC A coluna `valor_compra` aparece apenas do lado da bronze porque foi descartada, e `cnpj_digitos`
# MAGIC apenas do lado da silver porque foi criada no pipeline. A ausencia de complemento permanece, por
# MAGIC ser caracteristica do dado e nao defeito.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistencia: antes e depois
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
# MAGIC Resultado esperado: zero em todas as colunas de erro, duas unidades de medida (litro e metro
# MAGIC cubico) e uma unica forma para sem numero. Na bronze esses mesmos testes apontavam 253 precos fora
# MAGIC do padrao, 422.418 CNPJ com espaco, tres grafias de unidade e quatro formas de sem numero.

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
# MAGIC Verifica se toda chave da fato encontra a sua dimensao. Em um modelo dimensional, chave orfa e o
# MAGIC defeito mais caro: a consulta nao falha, ela simplesmente deixa linhas de fora e devolve um numero
# MAGIC menor, que parece plausivel.

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
# MAGIC ## Conservacao do volume ao longo do pipeline
# MAGIC
# MAGIC Cada camada precisa explicar a diferenca de linhas em relacao a anterior. Diferenca sem explicacao
# MAGIC e perda de dado.

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
# MAGIC Os precos extremos continuam na base. A consulta mostra onde eles estao: se estivessem concentrados
# MAGIC em um unico posto ou em uma unica data, seriam suspeita de erro de digitacao. Espalhados por
# MAGIC estados de custo logistico alto, sao diferenca regional legitima.
# MAGIC
# MAGIC A coluna decisiva e o percentual, nao a contagem. Sao Paulo tem muito mais postos pesquisados que
# MAGIC os estados do Norte, entao aparece no topo de qualquer contagem absoluta sem que isso signifique
# MAGIC preco alto. Ordenar pelo percentual dentro do proprio estado corrige essa distorcao.

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
# MAGIC | Problema detectado na bronze | Situacao nas tabelas finais |
# MAGIC |---|---|
# MAGIC | `valor_compra` inteiramente vazia | Resolvido: coluna descartada, motivo no catalogo |
# MAGIC | Preco como texto, em tres formatos diferentes | Resolvido: decimal(6,3), nenhuma conversao perdida |
# MAGIC | Data como texto | Resolvido: tipo date |
# MAGIC | CNPJ com espaco a esquerda em um dos arquivos | Resolvido, com chave natural so de digitos |
# MAGIC | Duas grafias para a unidade do GNV | Resolvido: uma unica grafia |
# MAGIC | Quatro formas de "sem numero" | Resolvido: forma unica S/N |
# MAGIC | 6 linhas duplicadas | Resolvido: removidas |
# MAGIC | Nome de municipio repetido entre estados | Tratado na modelagem: localidade sempre por estado e municipio |
# MAGIC | Posto com mais de uma bandeira no periodo | Tratado na modelagem: bandeira na fato, referente a data da coleta |
# MAGIC | Complemento de endereco ausente | Mantido: caracteristica do dado, nao defeito |
# MAGIC | Precos extremos | Mantidos: diferenca regional real, com intervalo documentado |
