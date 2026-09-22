# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Perfil de qualidade da camada bronze
# MAGIC
# MAGIC Investiga o dado como ele chegou, antes de qualquer correcao. O enunciado pede essa verificacao
# MAGIC "no processo inicial de captura", e a ordem importa: sao os problemas encontrados aqui que
# MAGIC definem o que a camada silver precisa corrigir. Limpar antes de medir significa corrigir no
# MAGIC escuro e nao ter como provar o que foi resolvido.
# MAGIC
# MAGIC As cinco dimensoes verificadas, uma secao para cada:
# MAGIC
# MAGIC | Dimensao | Pergunta |
# MAGIC |---|---|
# MAGIC | Completude | Ha nulos ou vazios? Em que proporcao? |
# MAGIC | Consistencia | Os valores seguem o formato esperado? |
# MAGIC | Unicidade | Ha duplicatas onde nao deveria? |
# MAGIC | Acuracia | Os valores fazem sentido no contexto? |
# MAGIC | Outliers | Ha valores extremos que distorcem a analise? |
# MAGIC
# MAGIC O resultado da completude fica gravado em uma tabela, para servir de comparacao depois que o
# MAGIC pipeline rodar (etapa de qualidade pos-pipeline).

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
# MAGIC Conta, para cada coluna, quantos valores estao nulos e quantos estao preenchidos com texto vazio.
# MAGIC Os dois casos sao tratados separadamente de proposito: o CSV entrega campo ausente como texto
# MAGIC vazio, nao como nulo, e quem so procura nulo conclui que a base esta completa.

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
# MAGIC ## 2. Consistencia
# MAGIC
# MAGIC Verifica se cada campo segue o formato que a documentacao da ANP descreve.

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
# MAGIC Unidade de medida por produto. O GNV e vendido em metro cubico e os demais em litro, entao
# MAGIC preco de GNV nao pode entrar na mesma media dos combustiveis liquidos.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT produto, unidade_medida, count(*) AS linhas
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY produto, unidade_medida
# MAGIC ORDER BY produto;

# COMMAND ----------

# MAGIC %md
# MAGIC Bandeiras registradas. A lista mostra nomes proximos que precisam de decisao explicita
# MAGIC (por exemplo, uma rede e a joint venture dela aparecem separadas).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT bandeira, count(*) AS linhas, count(DISTINCT cnpj_revenda) AS postos
# MAGIC FROM combustiveis.bronze.precos_anp
# MAGIC GROUP BY bandeira
# MAGIC ORDER BY linhas DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Nome de municipio nao e chave: o mesmo nome aparece em estados diferentes. A comparacao abaixo
# MAGIC mostra por que a chave de localidade precisa ser o par estado mais municipio.

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
# MAGIC Duas verificacoes diferentes: linhas identicas repetidas, e repeticao da chave de negocio
# MAGIC (um posto nao deveria ter dois precos do mesmo produto na mesma data de coleta).

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
# MAGIC Um posto pode aparecer com mais de uma bandeira ao longo do periodo. Isso nao e erro: o posto
# MAGIC trocou de distribuidora. Mas tem consequencia na modelagem, porque a bandeira deixa de ser um
# MAGIC atributo fixo do posto e passa a valer para a data da coleta.

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
# MAGIC ## 4. Acuracia
# MAGIC
# MAGIC Os precos fazem sentido para o mercado brasileiro no periodo? A conversao para numero aqui e
# MAGIC apenas para medir; nada e gravado. A tipagem definitiva acontece na camada silver.

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
# MAGIC Cobertura temporal: as coletas precisam cobrir os doze meses do recorte, sem mes faltando.

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
# MAGIC Marca os precos fora do intervalo entre o primeiro e o nonagesimo nono percentil de cada produto.
# MAGIC Extremo nao e sinonimo de errado: posto de rodovia em regiao remota cobra mais mesmo. Por isso o
# MAGIC objetivo aqui e localizar e dimensionar, nao excluir.

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
# MAGIC ## Resumo dos problemas e decisao de tratamento
# MAGIC
# MAGIC Cada problema encontrado acima vira uma transformacao na camada silver ou uma decisao de modelagem.
# MAGIC Os numeros exatos estao nos resultados das celulas; esta tabela registra o que sera feito com cada um.
# MAGIC
# MAGIC | Problema | Dimensao | Tratamento na silver |
# MAGIC |---|---|---|
# MAGIC | `valor_compra` inteiramente ausente (a ANP descontinuou a coleta em agosto de 2020) | Completude | Coluna descartada, com o motivo registrado no catalogo. Manter coluna vazia induz a erro |
# MAGIC | `complemento` ausente na maior parte das linhas | Completude | Mantida como esta: complemento opcional em endereco nao e defeito |
# MAGIC | Preco como texto com virgula decimal | Consistencia | Convertido para decimal |
# MAGIC | Data como texto em dd/mm/aaaa | Consistencia | Convertida para date |
# MAGIC | CNPJ com espaco a esquerda e com mascara | Consistencia | Espaco removido; mascara mantida para leitura humana e uma versao so com digitos usada como chave |
# MAGIC | `numero_rua` com S/N, SN, S N e variantes | Consistencia | Padronizado como S/N quando nao for numero |
# MAGIC | GNV medido em metro cubico | Consistencia | Unidade preservada e sinalizada; comparacoes de preco separam GNV dos liquidos |
# MAGIC | Linhas duplicadas exatas | Unicidade | Removidas |
# MAGIC | Mesmo nome de municipio em estados diferentes | Chave | Chave de localidade formada pelo par estado e municipio |
# MAGIC | Posto com mais de uma bandeira no periodo | Modelagem | Bandeira tratada como atributo da coleta, nao do posto |
# MAGIC | Precos extremos | Outliers | Mantidos, com o intervalo documentado. Excluir sem motivo apaga diferenca regional real |
