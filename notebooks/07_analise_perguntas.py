# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - Analise: respostas as perguntas do objetivo
# MAGIC
# MAGIC Uma consulta para cada pergunta definida na etapa de objetivo, usando o modelo estrela da camada
# MAGIC gold. As perguntas nao foram alteradas depois de ver os dados.
# MAGIC
# MAGIC | # | Pergunta | Decisao que ela apoia |
# MAGIC |---|---|---|
# MAGIC | P1 | Como o preco medio de cada combustivel evoluiu mes a mes entre julho de 2025 e junho de 2026? | Revisao do orcamento e do reembolso |
# MAGIC | P2 | Quais estados tem a gasolina comum e o diesel S10 mais caros e mais baratos? | Reembolso por estado e onde abastecer em viagem longa |
# MAGIC | P3 | Em quais estados compensa abastecer carro flex com etanol? | Politica de combustivel por estado |
# MAGIC | P4 | Postos bandeirados cobram mais que postos de bandeira branca? | Autorizar ou nao bandeira branca |
# MAGIC | P5 | Qual a diferenca de preco dentro do mesmo municipio na mesma semana? | Onde negociar convenio com postos |
# MAGIC
# MAGIC Uma regra vale para todas as consultas: o GNV fica de fora das comparacoes por litro, porque e
# MAGIC medido em metro cubico. A coluna `comparavel_por_litro` da dimensao de produto existe para que essa
# MAGIC regra nao dependa de quem escreve a consulta lembrar dela.

# COMMAND ----------

# MAGIC %md
# MAGIC ## P1 - Evolucao mensal do preco por combustivel
# MAGIC
# MAGIC Media do preco coletado por mes e por combustivel. A contagem de coletas aparece ao lado para deixar
# MAGIC claro sobre quantas observacoes cada media foi calculada.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.ano_mes,
# MAGIC        p.produto,
# MAGIC        round(avg(f.valor_venda), 3) AS preco_medio,
# MAGIC        count(*) AS coletas
# MAGIC FROM combustiveis.gold.fato_preco_coleta f
# MAGIC JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC JOIN combustiveis.gold.dim_tempo t ON t.data = f.data_coleta
# MAGIC WHERE p.comparavel_por_litro
# MAGIC GROUP BY t.ano_mes, p.produto
# MAGIC ORDER BY p.produto, t.ano_mes;

# COMMAND ----------

# MAGIC %md
# MAGIC Variacao entre o primeiro e o ultimo mes do periodo, por combustivel.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH medias AS (
# MAGIC   SELECT p.produto, t.ano_mes, avg(f.valor_venda) AS preco
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_tempo t ON t.data = f.data_coleta
# MAGIC   WHERE p.comparavel_por_litro
# MAGIC   GROUP BY p.produto, t.ano_mes
# MAGIC ),
# MAGIC extremos AS (
# MAGIC   SELECT produto,
# MAGIC          min_by(preco, ano_mes) AS preco_inicial,
# MAGIC          max_by(preco, ano_mes) AS preco_final,
# MAGIC          min(preco) AS menor_media_mensal,
# MAGIC          max(preco) AS maior_media_mensal
# MAGIC   FROM medias GROUP BY produto
# MAGIC )
# MAGIC SELECT produto,
# MAGIC        round(preco_inicial, 3) AS jul_2025,
# MAGIC        round(preco_final, 3) AS jun_2026,
# MAGIC        round(preco_final - preco_inicial, 3) AS variacao_reais,
# MAGIC        round(100.0 * (preco_final - preco_inicial) / preco_inicial, 2) AS variacao_pct,
# MAGIC        round(menor_media_mensal, 3) AS menor_mes,
# MAGIC        round(maior_media_mensal, 3) AS maior_mes
# MAGIC FROM extremos
# MAGIC ORDER BY produto;

# COMMAND ----------

# MAGIC %md
# MAGIC ## P2 - Estados mais caros e mais baratos
# MAGIC
# MAGIC Media por estado para a gasolina comum e o diesel S10, os dois combustiveis que a frota usa. A
# MAGIC posicao de cada estado aparece como numero de ordem, e a ultima coluna mostra quanto o estado esta
# MAGIC acima ou abaixo da media nacional.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_estado AS (
# MAGIC   SELECT p.produto,
# MAGIC          d.estado_sigla,
# MAGIC          d.regiao_sigla,
# MAGIC          avg(f.valor_venda) AS preco_medio,
# MAGIC          count(*) AS coletas
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   WHERE p.produto IN ('GASOLINA', 'DIESEL S10')
# MAGIC   GROUP BY p.produto, d.estado_sigla, d.regiao_sigla
# MAGIC ),
# MAGIC nacional AS (
# MAGIC   SELECT produto, avg(preco_medio) AS media_nacional FROM por_estado GROUP BY produto
# MAGIC )
# MAGIC SELECT e.produto,
# MAGIC        rank() OVER (PARTITION BY e.produto ORDER BY e.preco_medio DESC) AS posicao,
# MAGIC        e.estado_sigla,
# MAGIC        e.regiao_sigla,
# MAGIC        round(e.preco_medio, 3) AS preco_medio,
# MAGIC        e.coletas,
# MAGIC        round(100.0 * (e.preco_medio - n.media_nacional) / n.media_nacional, 2) AS dif_pct_para_media
# MAGIC FROM por_estado e
# MAGIC JOIN nacional n ON n.produto = e.produto
# MAGIC ORDER BY e.produto, posicao;

# COMMAND ----------

# MAGIC %md
# MAGIC Distancia entre o estado mais caro e o mais barato, por combustivel. E o numero que interessa para
# MAGIC decidir se o reembolso deve variar por estado.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_estado AS (
# MAGIC   SELECT p.produto, d.estado_sigla, avg(f.valor_venda) AS preco_medio
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   WHERE p.produto IN ('GASOLINA', 'DIESEL S10')
# MAGIC   GROUP BY p.produto, d.estado_sigla
# MAGIC )
# MAGIC SELECT produto,
# MAGIC        max_by(estado_sigla, preco_medio) AS estado_mais_caro,
# MAGIC        round(max(preco_medio), 3) AS preco_mais_caro,
# MAGIC        min_by(estado_sigla, preco_medio) AS estado_mais_barato,
# MAGIC        round(min(preco_medio), 3) AS preco_mais_barato,
# MAGIC        round(max(preco_medio) - min(preco_medio), 3) AS diferenca_reais,
# MAGIC        round(100.0 * (max(preco_medio) - min(preco_medio)) / min(preco_medio), 2) AS diferenca_pct
# MAGIC FROM por_estado
# MAGIC GROUP BY produto;

# COMMAND ----------

# MAGIC %md
# MAGIC ## P3 - Onde compensa abastecer com etanol
# MAGIC
# MAGIC O etanol rende cerca de 70% do que a gasolina rende por litro, entao so compensa quando custa ate
# MAGIC 70% do preco dela. A consulta calcula essa razao por estado no periodo inteiro.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH precos AS (
# MAGIC   SELECT d.estado_sigla,
# MAGIC          avg(CASE WHEN p.produto = 'ETANOL' THEN f.valor_venda END) AS etanol,
# MAGIC          avg(CASE WHEN p.produto = 'GASOLINA' THEN f.valor_venda END) AS gasolina,
# MAGIC          count(CASE WHEN p.produto = 'ETANOL' THEN 1 END) AS coletas_etanol
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   WHERE p.produto IN ('ETANOL', 'GASOLINA')
# MAGIC   GROUP BY d.estado_sigla
# MAGIC )
# MAGIC SELECT estado_sigla,
# MAGIC        round(etanol, 3) AS preco_etanol,
# MAGIC        round(gasolina, 3) AS preco_gasolina,
# MAGIC        round(etanol / gasolina, 4) AS razao,
# MAGIC        CASE WHEN etanol / gasolina <= 0.70 THEN 'COMPENSA' ELSE 'NAO COMPENSA' END AS decisao,
# MAGIC        coletas_etanol
# MAGIC FROM precos
# MAGIC WHERE coletas_etanol > 0
# MAGIC ORDER BY razao;

# COMMAND ----------

# MAGIC %md
# MAGIC A mesma razao mes a mes, nos estados onde a media anual ficou mais proxima do limite. Serve para
# MAGIC saber se a decisao e estavel durante o ano ou se muda com a safra da cana.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH mensal AS (
# MAGIC   SELECT d.estado_sigla,
# MAGIC          t.ano_mes,
# MAGIC          avg(CASE WHEN p.produto = 'ETANOL' THEN f.valor_venda END) AS etanol,
# MAGIC          avg(CASE WHEN p.produto = 'GASOLINA' THEN f.valor_venda END) AS gasolina
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   JOIN combustiveis.gold.dim_tempo t ON t.data = f.data_coleta
# MAGIC   WHERE p.produto IN ('ETANOL', 'GASOLINA')
# MAGIC   GROUP BY d.estado_sigla, t.ano_mes
# MAGIC )
# MAGIC SELECT estado_sigla,
# MAGIC        round(avg(etanol / gasolina), 4) AS razao_media_ano,
# MAGIC        sum(CASE WHEN etanol / gasolina <= 0.70 THEN 1 ELSE 0 END) AS meses_compensando,
# MAGIC        count(*) AS meses_com_dado,
# MAGIC        round(min(etanol / gasolina), 4) AS melhor_mes,
# MAGIC        round(max(etanol / gasolina), 4) AS pior_mes
# MAGIC FROM mensal
# MAGIC WHERE etanol IS NOT NULL AND gasolina IS NOT NULL
# MAGIC GROUP BY estado_sigla
# MAGIC HAVING sum(CASE WHEN etanol / gasolina <= 0.70 THEN 1 ELSE 0 END) > 0
# MAGIC ORDER BY meses_compensando DESC, razao_media_ano;

# COMMAND ----------

# MAGIC %md
# MAGIC ## P4 - Bandeirado contra bandeira branca
# MAGIC
# MAGIC A comparacao e feita **dentro de cada estado**, e nao na media nacional. Os dois tipos de posto nao
# MAGIC estao distribuidos igualmente pelo pais: se a bandeira branca for mais comum em estados baratos, uma
# MAGIC media nacional atribuiria a marca uma diferenca que e, na verdade, geografica. Comparar dentro do
# MAGIC mesmo estado remove esse efeito.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_estado_tipo AS (
# MAGIC   SELECT d.estado_sigla,
# MAGIC          b.tipo_bandeira,
# MAGIC          avg(f.valor_venda) AS preco_medio,
# MAGIC          count(*) AS coletas
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_bandeira b ON b.sk_bandeira = f.sk_bandeira
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   WHERE p.produto = 'GASOLINA'
# MAGIC   GROUP BY d.estado_sigla, b.tipo_bandeira
# MAGIC ),
# MAGIC comparacao AS (
# MAGIC   SELECT estado_sigla,
# MAGIC          max(CASE WHEN tipo_bandeira = 'BANDEIRADO' THEN preco_medio END) AS bandeirado,
# MAGIC          max(CASE WHEN tipo_bandeira = 'BRANCA' THEN preco_medio END) AS branca,
# MAGIC          max(CASE WHEN tipo_bandeira = 'BANDEIRADO' THEN coletas END) AS coletas_bandeirado,
# MAGIC          max(CASE WHEN tipo_bandeira = 'BRANCA' THEN coletas END) AS coletas_branca
# MAGIC   FROM por_estado_tipo GROUP BY estado_sigla
# MAGIC )
# MAGIC SELECT estado_sigla,
# MAGIC        round(bandeirado, 3) AS preco_bandeirado,
# MAGIC        round(branca, 3) AS preco_branca,
# MAGIC        round(bandeirado - branca, 3) AS diferenca_reais,
# MAGIC        round(100.0 * (bandeirado - branca) / branca, 2) AS diferenca_pct,
# MAGIC        coletas_bandeirado,
# MAGIC        coletas_branca
# MAGIC FROM comparacao
# MAGIC WHERE bandeirado IS NOT NULL AND branca IS NOT NULL
# MAGIC ORDER BY diferenca_pct DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Resumo da P4: em quantos estados o posto de bandeira branca sai mais barato, e qual a diferenca
# MAGIC tipica. A mediana entra ao lado da media porque um unico estado com diferenca extrema puxaria a
# MAGIC media sozinho.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_estado_tipo AS (
# MAGIC   SELECT d.estado_sigla, b.tipo_bandeira, avg(f.valor_venda) AS preco_medio
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_bandeira b ON b.sk_bandeira = f.sk_bandeira
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   WHERE p.produto = 'GASOLINA'
# MAGIC   GROUP BY d.estado_sigla, b.tipo_bandeira
# MAGIC ),
# MAGIC comparacao AS (
# MAGIC   SELECT estado_sigla,
# MAGIC          max(CASE WHEN tipo_bandeira = 'BANDEIRADO' THEN preco_medio END) AS bandeirado,
# MAGIC          max(CASE WHEN tipo_bandeira = 'BRANCA' THEN preco_medio END) AS branca
# MAGIC   FROM por_estado_tipo GROUP BY estado_sigla
# MAGIC )
# MAGIC SELECT count(*) AS estados_comparados,
# MAGIC        sum(CASE WHEN branca < bandeirado THEN 1 ELSE 0 END) AS estados_branca_mais_barata,
# MAGIC        round(avg(bandeirado - branca), 3) AS diferenca_media_reais,
# MAGIC        round(percentile_approx(bandeirado - branca, 0.5), 3) AS diferenca_mediana_reais,
# MAGIC        round(avg(100.0 * (bandeirado - branca) / branca), 2) AS diferenca_media_pct
# MAGIC FROM comparacao
# MAGIC WHERE bandeirado IS NOT NULL AND branca IS NOT NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## P5 - Dispersao dentro do mesmo municipio
# MAGIC
# MAGIC Compara o posto mais barato com o mais caro do mesmo municipio, na mesma semana e para o mesmo
# MAGIC combustivel. Tres cuidados na consulta:
# MAGIC
# MAGIC 1. A semana e identificada pela data da segunda-feira, e nao por um rotulo de numero de semana.
# MAGIC 2. So entram grupos com pelo menos cinco postos pesquisados; com dois ou tres postos, a amplitude
# MAGIC    diz mais sobre a amostra do que sobre o mercado local.
# MAGIC 3. O resultado por municipio e a media das amplitudes semanais, e nao a amplitude do ano inteiro,
# MAGIC    que misturaria variacao de preco no tempo com diferenca entre postos.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_municipio_semana AS (
# MAGIC   SELECT d.estado_sigla,
# MAGIC          d.municipio,
# MAGIC          t.semana_inicio,
# MAGIC          count(DISTINCT f.sk_posto) AS postos,
# MAGIC          min(f.valor_venda) AS mais_barato,
# MAGIC          max(f.valor_venda) AS mais_caro,
# MAGIC          avg(f.valor_venda) AS media
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   JOIN combustiveis.gold.dim_tempo t ON t.data = f.data_coleta
# MAGIC   WHERE p.produto = 'GASOLINA'
# MAGIC   GROUP BY d.estado_sigla, d.municipio, t.semana_inicio
# MAGIC   HAVING count(DISTINCT f.sk_posto) >= 5
# MAGIC )
# MAGIC SELECT estado_sigla,
# MAGIC        municipio,
# MAGIC        count(*) AS semanas,
# MAGIC        round(avg(postos), 1) AS postos_por_semana,
# MAGIC        round(avg(mais_caro - mais_barato), 3) AS amplitude_media_reais,
# MAGIC        round(100.0 * avg((mais_caro - mais_barato) / media), 2) AS amplitude_media_pct
# MAGIC FROM por_municipio_semana
# MAGIC GROUP BY estado_sigla, municipio
# MAGIC HAVING count(*) >= 20
# MAGIC ORDER BY amplitude_media_reais DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC Visao geral da P5: quanto se ganha, em media, escolhendo o posto mais barato do municipio em vez de
# MAGIC abastecer no primeiro que aparecer.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_municipio_semana AS (
# MAGIC   SELECT d.municipio,
# MAGIC          t.semana_inicio,
# MAGIC          min(f.valor_venda) AS mais_barato,
# MAGIC          max(f.valor_venda) AS mais_caro,
# MAGIC          avg(f.valor_venda) AS media
# MAGIC   FROM combustiveis.gold.fato_preco_coleta f
# MAGIC   JOIN combustiveis.gold.dim_produto p ON p.sk_produto = f.sk_produto
# MAGIC   JOIN combustiveis.gold.dim_posto d ON d.sk_posto = f.sk_posto
# MAGIC   JOIN combustiveis.gold.dim_tempo t ON t.data = f.data_coleta
# MAGIC   WHERE p.produto = 'GASOLINA'
# MAGIC   GROUP BY d.municipio, t.semana_inicio
# MAGIC   HAVING count(DISTINCT f.sk_posto) >= 5
# MAGIC )
# MAGIC SELECT count(*) AS grupos_municipio_semana,
# MAGIC        round(avg(mais_caro - mais_barato), 3) AS amplitude_media_reais,
# MAGIC        round(percentile_approx(mais_caro - mais_barato, 0.5), 3) AS amplitude_mediana_reais,
# MAGIC        round(avg(media - mais_barato), 3) AS economia_media_contra_a_media_local,
# MAGIC        round(100.0 * avg((media - mais_barato) / media), 2) AS economia_media_pct
# MAGIC FROM por_municipio_semana;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limitacoes das respostas
# MAGIC
# MAGIC Tres pontos que valem para todas as perguntas e que precisam acompanhar qualquer decisao tomada a
# MAGIC partir delas:
# MAGIC
# MAGIC 1. **A pesquisa e amostral.** A ANP cobre uma amostra de postos em pouco mais de 400 municipios, e
# MAGIC    nao todos os postos do pais. As conclusoes valem para os municipios pesquisados.
# MAGIC 2. **Preco de bomba nao e preco negociado.** Frota com contrato ou cartao combustivel costuma pagar
# MAGIC    valor diferente do exibido na placa, que e o que a pesquisa registra.
# MAGIC 3. **Media esconde distribuicao.** Um estado com preco medio baixo pode ter municipios caros. Por
# MAGIC    isso a P5 olha o municipio, e nao so o estado.
