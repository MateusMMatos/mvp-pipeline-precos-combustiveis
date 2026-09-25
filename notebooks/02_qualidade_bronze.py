# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Perfil de qualidade da camada bronze
# MAGIC
# MAGIC Analisa os dados como chegaram, antes de qualquer correção. O enunciado pede essa verificação no
# MAGIC processo inicial de captura, e são os problemas encontrados aqui que definem as transformações da
# MAGIC camada silver. Medir antes de corrigir também permite demonstrar depois o que foi resolvido.
# MAGIC
# MAGIC Dimensões verificadas:
# MAGIC
# MAGIC | Dimensão | Pergunta |
# MAGIC |---|---|
# MAGIC | Completude | Há nulos ou vazios? Em que proporção? |
# MAGIC | Consistência | Os valores seguem o formato esperado? |
# MAGIC | Unicidade | Há duplicatas onde não deveria haver? |
# MAGIC | Acurácia | Os valores fazem sentido no contexto? |
# MAGIC | Outliers | Há valores extremos que distorcem a análise? |
# MAGIC
# MAGIC O resultado da completude é gravado em uma tabela, usada depois na comparação com as tabelas finais
# MAGIC (notebook 05).

# COMMAND ----------

from pyspark.sql import functions as F

TABELA_BRONZE = "combustiveis.bronze.precos_anp"

bronze = spark.table(TABELA_BRONZE)
colunas_origem = [c for c in bronze.columns if not c.startswith("_")]
total_linhas = bronze.count()

print(f"Tabela: {TABELA_BRONZE}")
print(f"Linhas: {total_linhas}")
print(f"Colunas da fonte: {len(colunas_origem)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude
# MAGIC
# MAGIC Conta, para cada coluna, os valores nulos e os preenchidos com texto vazio. Os dois casos são contados
# MAGIC separadamente porque, em arquivos CSV, um campo ausente pode chegar como texto vazio, e não como
# MAGIC nulo.

# COMMAND ----------

expressoes = []
for c in colunas_origem:
    expressoes.append(F.count(F.when(F.col(c).isNull(), c)).alias(f"{c}__nulos"))
    expressoes.append(F.count(F.when(F.trim(F.col(c)) == "", c)).alias(f"{c}__vazios"))

medidas = bronze.agg(*expressoes).collect()[0].asDict()

linhas_perfil = []
for c in colunas_origem:
    nulos = medidas[f"{c}__nulos"]
    vazios = medidas[f"{c}__vazios"]
    ausentes = nulos + vazios
    linhas_perfil.append((c, nulos, vazios, round(100.0 * ausentes / total_linhas, 2)))

perfil_completude = spark.createDataFrame(
    linhas_perfil, "coluna string, nulos bigint, vazios bigint, pct_ausente double"
).orderBy(F.desc("pct_ausente"))

perfil_completude.write.mode("overwrite").saveAsTable("combustiveis.bronze.perfil_completude")
display(perfil_completude)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consistência
# MAGIC
# MAGIC Verifica se cada campo segue o formato descrito na documentação da ANP.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*) AS total,
# MAGIC   sum(CASE WHEN to_date(data_coleta, 'dd/MM/yyyy') IS NULL THEN 1 ELSE 0 END) AS data_fora_do_formato,
# MAGIC   sum(CASE WHEN NOT valor_venda RLIKE '^[0-9]+,[0-9]+$' THEN 1 ELSE 0 END) AS preco_fora_do_formato,
# MAGIC   sum(CASE WHEN NOT cep RLIKE '^[0-9]{5}-[0-9]{3}$' THEN 1 ELSE 0 END) AS cep_fora_do_formato,
# MAGIC   sum(CASE WHEN cnpj_revenda <> trim(cnpj_revenda) THEN 1 ELSE 0 END) AS cnpj_com_espaco,
# MAGIC   sum(CASE WHEN NOT numero_rua RLIKE '^[0-9]+$' THEN 1 ELSE 0 END) AS numero_rua_nao_numerico
# MAGIC FROM combustiveis.bronze.precos_anp;

# COMMAND ----------

# MAGIC %md
# MAGIC Unidade de medida por produto. O GNV é vendido em metro cúbico e os demais combustíveis em litro, por
# MAGIC isso o preço do GNV não pode entrar na mesma média dos combustíveis líquidos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT produto, unidade_medida, count(*) AS linhas
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY produto, unidade_medida
# MAGIC ORDER BY produto;

# COMMAND ----------

# MAGIC %md
# MAGIC Bandeiras registradas. A lista inclui nomes próximos, como RAIZEN e RAIZEN MIME, que exigem uma
# MAGIC decisão explícita sobre agrupamento.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT bandeira, count(*) AS linhas, count(DISTINCT cnpj_revenda) AS postos
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY bandeira
# MAGIC ORDER BY linhas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC O nome do município não serve como chave, porque o mesmo nome aparece em estados diferentes. A
# MAGIC comparação a seguir mostra por que a localidade precisa ser identificada pelo par estado e
# MAGIC município.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(DISTINCT municipio) AS nomes_de_municipio,
# MAGIC   count(DISTINCT concat(estado_sigla, '|', municipio)) AS pares_estado_municipio
# MAGIC FROM combustiveis.bronze.precos_anp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT municipio, count(DISTINCT estado_sigla) AS estados,
# MAGIC        concat_ws(', ', collect_set(estado_sigla)) AS quais
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY municipio
# MAGIC HAVING count(DISTINCT estado_sigla) > 1
# MAGIC ORDER BY municipio;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unicidade
# MAGIC
# MAGIC Duas verificações: linhas idênticas repetidas e repetição da chave de negócio (um posto não deveria
# MAGIC ter dois preços do mesmo produto na mesma data de coleta).

# COMMAND ----------

colunas_sem_controle = bronze.select(*colunas_origem)
duplicatas_exatas = total_linhas - colunas_sem_controle.distinct().count()

chave_negocio = (
    bronze.select(
        F.trim("cnpj_revenda").alias("cnpj"),
        "produto",
        "data_coleta",
    )
    .distinct()
    .count()
)
duplicatas_chave = total_linhas - chave_negocio

print(f"Linhas duplicadas exatas: {duplicatas_exatas}")
print(f"Repeticoes da chave (cnpj, produto, data): {duplicatas_chave}")

# COMMAND ----------

# MAGIC %md
# MAGIC Um posto pode aparecer com mais de uma bandeira ao longo do período quando troca de distribuidora.
# MAGIC Isso não é um erro, mas afeta a modelagem: a bandeira deixa de ser um atributo fixo do posto e passa a
# MAGIC valer para a data da coleta.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS postos_com_mais_de_uma_bandeira
# MAGIC FROM (
# MAGIC   SELECT trim(cnpj_revenda) AS cnpj
# MAGIC   FROM combustiveis.bronze.precos_anp
# MAGIC   GROUP BY trim(cnpj_revenda)
# MAGIC   HAVING count(DISTINCT bandeira) > 1
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Acurácia
# MAGIC
# MAGIC Verifica se os preços são plausíveis para o mercado brasileiro no período. A conversão para número
# MAGIC serve apenas para a medição e não é gravada; a tipagem definitiva é feita na camada silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   produto,
# MAGIC   count(*) AS linhas,
# MAGIC   round(min(cast(replace(valor_venda, ',', '.') AS double)), 2) AS minimo,
# MAGIC   round(percentile_approx(cast(replace(valor_venda, ',', '.') AS double), 0.5), 2) AS mediana,
# MAGIC   round(max(cast(replace(valor_venda, ',', '.') AS double)), 2) AS maximo,
# MAGIC   sum(CASE WHEN cast(replace(valor_venda, ',', '.') AS double) <= 0 THEN 1 ELSE 0 END) AS precos_nao_positivos
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY produto
# MAGIC ORDER BY produto;

# COMMAND ----------

# MAGIC %md
# MAGIC Cobertura temporal: as coletas precisam cobrir os doze meses do recorte, sem meses faltando.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT date_format(to_date(data_coleta, 'dd/MM/yyyy'), 'yyyy-MM') AS mes,
# MAGIC        count(*) AS coletas,
# MAGIC        count(DISTINCT to_date(data_coleta, 'dd/MM/yyyy')) AS dias_com_coleta
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outliers
# MAGIC
# MAGIC Identifica os preços fora do intervalo entre o percentil 1 e o percentil 99 de cada produto. Valores
# MAGIC extremos não são necessariamente erros, já que postos em regiões remotas tendem a cobrar mais; o
# MAGIC objetivo aqui é localizar e dimensionar esses valores, e não excluí-los.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH precos AS (
# MAGIC   SELECT produto, cast(replace(valor_venda, ',', '.') AS double) AS preco
# MAGIC   FROM combustiveis.bronze.precos_anp
# MAGIC ),
# MAGIC limites AS (
# MAGIC   SELECT produto,
# MAGIC          percentile_approx(preco, 0.01) AS p01,
# MAGIC          percentile_approx(preco, 0.99) AS p99
# MAGIC   FROM precos GROUP BY produto
# MAGIC )
# MAGIC SELECT l.produto,
# MAGIC        round(l.p01, 2) AS p01,
# MAGIC        round(l.p99, 2) AS p99,
# MAGIC        sum(CASE WHEN p.preco < l.p01 OR p.preco > l.p99 THEN 1 ELSE 0 END) AS fora_do_intervalo,
# MAGIC        round(100.0 * sum(CASE WHEN p.preco < l.p01 OR p.preco > l.p99 THEN 1 ELSE 0 END) / count(*), 2) AS pct
# MAGIC FROM precos p
# MAGIC JOIN limites l ON p.produto = l.produto
# MAGIC GROUP BY l.produto, l.p01, l.p99
# MAGIC ORDER BY l.produto;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo dos problemas e decisão de tratamento
# MAGIC
# MAGIC Cada problema encontrado corresponde a uma transformação na camada silver ou a uma decisão de
# MAGIC modelagem. Os números estão nos resultados das células anteriores; a tabela registra o tratamento
# MAGIC previsto para cada um.
# MAGIC
# MAGIC | Problema | Dimensão | Tratamento |
# MAGIC |---|---|---|
# MAGIC | `valor_compra` sem nenhum valor (coleta encerrada pela ANP em agosto de 2020) | Completude | Coluna descartada, com o motivo registrado no catálogo |
# MAGIC | `complemento` ausente na maior parte das linhas | Completude | Mantida, por ser um campo opcional do endereço |
# MAGIC | Preço como texto com vírgula decimal | Consistência | Conversão para decimal |
# MAGIC | Data como texto em dd/mm/aaaa | Consistência | Conversão para date |
# MAGIC | CNPJ com espaço à esquerda e com máscara | Consistência | Remoção do espaço; máscara mantida para leitura e versão apenas com dígitos usada como chave |
# MAGIC | `numero_rua` com S/N, SN, S N e variantes | Consistência | Padronização como S/N quando não for número |
# MAGIC | GNV medido em metro cúbico | Consistência | Unidade preservada; as comparações de preço separam o GNV dos líquidos |
# MAGIC | Linhas duplicadas exatas | Unicidade | Remoção |
# MAGIC | Mesmo nome de município em estados diferentes | Chave | Localidade identificada pelo par estado e município |
# MAGIC | Posto com mais de uma bandeira no período | Modelagem | Bandeira tratada como atributo da coleta, e não do posto |
# MAGIC | Preços extremos | Outliers | Mantidos, com o intervalo documentado, por representarem diferenças regionais |
