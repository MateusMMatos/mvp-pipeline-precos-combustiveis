-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 - Catalogo de dados
-- MAGIC
-- MAGIC Grava a descricao de cada tabela e de cada coluna no Unity Catalog. O enunciado exige, para cada
-- MAGIC campo: nome e descricao, tipo de dado, dominio de valores (minimo e maximo para numericos,
-- MAGIC categorias possiveis para categoricos) e linhagem, isto e, de onde o dado veio e se houve
-- MAGIC transformacao ou juncao para compo-lo.
-- MAGIC
-- MAGIC O catalogo fica gravado na propria plataforma, e nao apenas em um documento a parte, por um motivo
-- MAGIC pratico: quem abre a tabela no Catalog Explorer ou escreve uma consulta ve a descricao ali, no
-- MAGIC momento em que precisa dela. Documentacao que mora longe do dado envelhece sem que ninguem perceba.
-- MAGIC
-- MAGIC Os dominios de valores citados referem-se ao periodo carregado: julho de 2025 a junho de 2026.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Camada bronze

-- COMMAND ----------

COMMENT ON TABLE combustiveis.bronze.precos_anp IS
'Precos de combustiveis da ANP como publicados, sem tratamento. Uma linha por preco coletado em um posto. Fonte: ANP, Serie Historica de Precos de Combustiveis, arquivos semestrais de jul-dez/2025 e jan-jun/2026, coletados por pesquisa semanal presencial em amostra de postos. Linhagem: carga direta dos arquivos CSV do volume combustiveis.bronze.arquivos_anp, sem alteracao de valores.';

COMMENT ON COLUMN combustiveis.bronze.precos_anp.regiao_sigla IS 'Sigla da regiao do posto, como publicada. Texto. Dominio: N, NE, CO, SE, S.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.estado_sigla IS 'Sigla da unidade federativa do posto. Texto. Dominio: as 27 UFs.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.municipio IS 'Nome do municipio do posto. Texto. 418 nomes distintos; nao e chave sozinho, porque VALENCA ocorre na BA e no RJ.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.revenda IS 'Razao social do posto pesquisado. Texto.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.cnpj_revenda IS 'CNPJ do posto, com mascara, como publicado. Texto. No arquivo de jan-jun/2026 vem com um espaco a esquerda em todas as linhas.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.nome_rua IS 'Logradouro do posto. Texto.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.numero_rua IS 'Numero do logradouro. Texto, porque a fonte usa S/N, SN, S N e variantes para posto sem numero.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.complemento IS 'Complemento do endereco. Texto. Ausente em 77,38% das linhas, o que e esperado para complemento.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.bairro IS 'Bairro do posto. Texto. Ausente em 0,17% das linhas.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.cep IS 'CEP do posto. Texto no formato 00000-000.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.produto IS 'Combustivel pesquisado. Texto. Dominio: GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.data_coleta IS 'Data da coleta do preco em campo. Texto em dd/mm/aaaa, como publicado. Dominio: 01/07/2025 a 30/06/2026.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.valor_venda IS 'Preco de venda ao consumidor. Texto com virgula decimal, como publicado; ha valores com duas casas, uma casa e sem virgula. Dominio: 2,89 a 9,99.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.valor_compra IS 'Preco de distribuicao. Texto. Integralmente vazia: a ANP encerrou essa coleta em agosto de 2020, conforme o dicionario oficial. Nao serve para calcular margem.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.unidade_medida IS 'Unidade do preco. Texto. Dominio: R$ / litro; R$ / m3 e R$ / m3 com expoente, duas grafias para o GNV.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp.bandeira IS 'Marca da distribuidora exibida pelo posto, ou BRANCA quando nao exibe marca. Texto. 49 valores distintos.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp._arquivo_origem IS 'Metadado de controle: nome do arquivo CSV de origem da linha.';
COMMENT ON COLUMN combustiveis.bronze.precos_anp._data_ingestao IS 'Metadado de controle: momento em que a linha foi carregada na bronze.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Camada silver

-- COMMAND ----------

COMMENT ON TABLE combustiveis.silver.precos IS
'Precos de combustiveis da ANP tipados, padronizados e sem duplicatas. Uma linha por preco coletado em um posto. Linhagem: derivada de combustiveis.bronze.precos_anp por oito transformacoes registradas em combustiveis.silver.log_transformacoes.';

COMMENT ON COLUMN combustiveis.silver.precos.regiao_sigla IS 'Sigla da regiao do posto. Texto. Dominio: N, NE, CO, SE, S. Linhagem: bronze, com remocao de espacos.';
COMMENT ON COLUMN combustiveis.silver.precos.estado_sigla IS 'Sigla da UF do posto. Texto. Dominio: as 27 UFs. Linhagem: bronze, com remocao de espacos.';
COMMENT ON COLUMN combustiveis.silver.precos.municipio IS 'Nome do municipio do posto. Texto. Linhagem: bronze, com remocao de espacos. Usar sempre junto com estado_sigla.';
COMMENT ON COLUMN combustiveis.silver.precos.razao_social IS 'Razao social do posto. Texto. Linhagem: coluna revenda da bronze, renomeada e sem espacos duplicados.';
COMMENT ON COLUMN combustiveis.silver.precos.cnpj IS 'CNPJ do posto com mascara, para leitura. Texto. Linhagem: bronze, com o espaco a esquerda removido.';
COMMENT ON COLUMN combustiveis.silver.precos.cnpj_digitos IS 'CNPJ apenas com digitos, chave natural do posto. Texto de 14 caracteres. Linhagem: derivada do CNPJ pela remocao de tudo que nao e digito.';
COMMENT ON COLUMN combustiveis.silver.precos.logradouro IS 'Logradouro do posto. Texto. Linhagem: coluna nome_rua da bronze, renomeada.';
COMMENT ON COLUMN combustiveis.silver.precos.numero IS 'Numero do logradouro. Texto. Variantes de sem numero unificadas em S/N. Linhagem: coluna numero_rua da bronze, padronizada.';
COMMENT ON COLUMN combustiveis.silver.precos.complemento IS 'Complemento do endereco. Texto, opcional.';
COMMENT ON COLUMN combustiveis.silver.precos.bairro IS 'Bairro do posto. Texto.';
COMMENT ON COLUMN combustiveis.silver.precos.cep IS 'CEP do posto. Texto no formato 00000-000.';
COMMENT ON COLUMN combustiveis.silver.precos.produto IS 'Combustivel pesquisado. Texto. Dominio: GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV.';
COMMENT ON COLUMN combustiveis.silver.precos.unidade_medida IS 'Unidade do preco. Texto. Dominio: R$ / litro, R$ / m3. Linhagem: grafias do GNV unificadas.';
COMMENT ON COLUMN combustiveis.silver.precos.bandeira IS 'Marca exibida pelo posto na data da coleta, ou BRANCA. Texto. 49 valores distintos.';
COMMENT ON COLUMN combustiveis.silver.precos.data_coleta IS 'Data da coleta em campo. Tipo date. Dominio: 2025-07-01 a 2026-06-30. Linhagem: texto dd/mm/aaaa da bronze convertido.';
COMMENT ON COLUMN combustiveis.silver.precos.valor_venda IS 'Preco de venda ao consumidor, na unidade do produto. decimal(6,3). Dominio: 2,890 a 9,990. Linhagem: texto com virgula decimal da bronze convertido.';
COMMENT ON COLUMN combustiveis.silver.precos._arquivo_origem IS 'Metadado de controle herdado da bronze: arquivo CSV de origem.';
COMMENT ON COLUMN combustiveis.silver.precos._data_ingestao IS 'Metadado de controle herdado da bronze: momento da carga na bronze.';
COMMENT ON COLUMN combustiveis.silver.precos._data_processamento_silver IS 'Metadado de controle: momento em que a linha foi processada nesta camada.';

COMMENT ON TABLE combustiveis.silver.log_transformacoes IS
'Registro das transformacoes aplicadas na camada silver, com o motivo e o numero de linhas afetadas por cada uma. Gerado pelo notebook 03 a cada execucao.';

COMMENT ON COLUMN combustiveis.silver.log_transformacoes.transformacao IS 'Nome da transformacao aplicada na camada silver. Texto. Dominio: as oito transformacoes do notebook 03.';
COMMENT ON COLUMN combustiveis.silver.log_transformacoes.motivo IS 'Problema de qualidade que motivou a transformacao, conforme medido no notebook 02. Texto.';
COMMENT ON COLUMN combustiveis.silver.log_transformacoes.linhas_afetadas IS 'Quantidade de linhas afetadas pela transformacao. Inteiro. Dominio observado: 6 a 806.626.';
COMMENT ON COLUMN combustiveis.silver.log_transformacoes._data_execucao IS 'Metadado de controle: momento da execucao que gerou o registro.';

COMMENT ON TABLE combustiveis.bronze.perfil_completude IS
'Perfil de completude da camada bronze: nulos, vazios e percentual de ausencia por coluna. Gerado pelo notebook 02, serve de base de comparacao para a verificacao de qualidade apos o pipeline.';

COMMENT ON COLUMN combustiveis.bronze.perfil_completude.coluna IS 'Nome da coluna da tabela bronze.precos_anp analisada. Texto. Dominio: as 16 colunas da fonte.';
COMMENT ON COLUMN combustiveis.bronze.perfil_completude.nulos IS 'Quantidade de valores nulos na coluna. Inteiro. Dominio observado: 0 a 806.626.';
COMMENT ON COLUMN combustiveis.bronze.perfil_completude.vazios IS 'Quantidade de valores preenchidos com texto vazio. Contada separadamente dos nulos porque arquivo CSV entrega campo ausente como texto vazio. Inteiro.';
COMMENT ON COLUMN combustiveis.bronze.perfil_completude.pct_ausente IS 'Percentual de linhas sem valor na coluna, somando nulos e vazios. Decimal. Dominio: 0 a 100.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Camada gold: tabela de fatos

-- COMMAND ----------

COMMENT ON TABLE combustiveis.gold.fato_preco_coleta IS
'Tabela de fatos do modelo estrela. Grao: um preco, de um combustivel, em um posto, em uma data de coleta. Medida: valor_venda. Linhagem: derivada de combustiveis.silver.precos por juncao com dim_produto (produto e unidade), dim_bandeira (bandeira) e dim_posto (cnpj_digitos), substituindo os atributos pelas chaves das dimensoes.';

COMMENT ON COLUMN combustiveis.gold.fato_preco_coleta.sk_posto IS 'Chave substituta do posto. Inteiro. Juncao com combustiveis.gold.dim_posto.';
COMMENT ON COLUMN combustiveis.gold.fato_preco_coleta.sk_produto IS 'Chave substituta do combustivel. Inteiro. Juncao com combustiveis.gold.dim_produto.';
COMMENT ON COLUMN combustiveis.gold.fato_preco_coleta.sk_bandeira IS 'Chave substituta da bandeira exibida na data da coleta. Inteiro. Fica na fato, e nao na dimensao do posto, porque 359 postos trocaram de bandeira no periodo.';
COMMENT ON COLUMN combustiveis.gold.fato_preco_coleta.data_coleta IS 'Data da coleta. Tipo date. Juncao com combustiveis.gold.dim_tempo. Dominio: 2025-07-01 a 2026-06-30.';
COMMENT ON COLUMN combustiveis.gold.fato_preco_coleta.valor_venda IS 'Medida: preco de venda ao consumidor, na unidade do produto. decimal(6,3). Dominio: 2,890 a 9,990. Precos de GNV estao em R$ por metro cubico e nao devem entrar na mesma media dos combustiveis liquidos.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Camada gold: dimensoes

-- COMMAND ----------

COMMENT ON TABLE combustiveis.gold.dim_posto IS
'Dimensao de postos revendedores, um registro por CNPJ. Guarda a versao cadastral mais recente observada no periodo (dimensao de mudanca lenta do tipo 1, sem historico dos atributos). Linhagem: derivada de combustiveis.silver.precos.';

COMMENT ON COLUMN combustiveis.gold.dim_posto.sk_posto IS 'Chave substituta do posto. Inteiro sequencial.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.cnpj_digitos IS 'Chave natural: CNPJ apenas com digitos. Texto de 14 caracteres.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.cnpj IS 'CNPJ com mascara, para leitura humana. Texto.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.razao_social IS 'Razao social do posto. Texto.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.logradouro IS 'Logradouro do posto. Texto.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.numero IS 'Numero do logradouro, ou S/N. Texto.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.complemento IS 'Complemento do endereco. Texto, opcional.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.bairro IS 'Bairro do posto. Texto.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.cep IS 'CEP do posto. Texto no formato 00000-000.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.municipio IS 'Municipio do posto. Texto. Deve ser usado com estado_sigla, porque ha nome de municipio repetido em estados diferentes.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.estado_sigla IS 'UF do posto. Texto. Dominio: as 27 UFs.';
COMMENT ON COLUMN combustiveis.gold.dim_posto.regiao_sigla IS 'Regiao do posto. Texto. Dominio: N, NE, CO, SE, S.';

COMMENT ON TABLE combustiveis.gold.dim_produto IS
'Dimensao de combustiveis, um registro por combinacao de produto e unidade de medida. Acrescenta a classificacao em grupo e a marcacao de comparabilidade por litro. Linhagem: derivada de combustiveis.silver.precos.';

COMMENT ON COLUMN combustiveis.gold.dim_produto.sk_produto IS 'Chave substituta do combustivel. Inteiro sequencial.';
COMMENT ON COLUMN combustiveis.gold.dim_produto.produto IS 'Nome do combustivel como a ANP publica. Texto. Dominio: GASOLINA, GASOLINA ADITIVADA, ETANOL, DIESEL, DIESEL S10, GNV.';
COMMENT ON COLUMN combustiveis.gold.dim_produto.unidade_medida IS 'Unidade do preco. Texto. Dominio: R$ / litro, R$ / m3.';
COMMENT ON COLUMN combustiveis.gold.dim_produto.grupo_combustivel IS 'Agrupamento do combustivel. Texto. Dominio: GASOLINA, ETANOL, DIESEL, GNV. Derivado do nome do produto.';
COMMENT ON COLUMN combustiveis.gold.dim_produto.comparavel_por_litro IS 'Indica se o preco pode entrar em comparacoes por litro. Booleano. Falso apenas para o GNV, medido em metro cubico.';

COMMENT ON TABLE combustiveis.gold.dim_bandeira IS
'Dimensao de bandeiras, um registro por marca exibida. Classifica o posto em bandeirado ou bandeira branca, conforme a definicao da ANP. Linhagem: derivada de combustiveis.silver.precos.';

COMMENT ON COLUMN combustiveis.gold.dim_bandeira.sk_bandeira IS 'Chave substituta da bandeira. Inteiro sequencial.';
COMMENT ON COLUMN combustiveis.gold.dim_bandeira.bandeira IS 'Marca da distribuidora exibida pelo posto, ou BRANCA. Texto. 49 valores distintos.';
COMMENT ON COLUMN combustiveis.gold.dim_bandeira.tipo_bandeira IS 'Classificacao da bandeira. Texto. Dominio: BANDEIRADO, BRANCA. Posto bandeirado exibe a marca de uma distribuidora e so vende o combustivel dela; posto de bandeira branca nao exibe marca.';

COMMENT ON TABLE combustiveis.gold.dim_tempo IS
'Dimensao de tempo, um registro por dia entre a primeira e a ultima coleta, incluindo dias sem coleta. Linhagem: gerada a partir dos limites de data de combustiveis.silver.precos.';

COMMENT ON COLUMN combustiveis.gold.dim_tempo.data IS 'Data. Chave da dimensao. Dominio: 2025-07-01 a 2026-06-30.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.ano IS 'Ano civil. Inteiro. Dominio: 2025, 2026.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.mes IS 'Mes do ano. Inteiro. Dominio: 1 a 12.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.dia IS 'Dia do mes. Inteiro. Dominio: 1 a 31.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.ano_mes IS 'Ano e mes no formato aaaa-mm. Texto. Usado nas series mensais.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.semana_inicio IS 'Segunda-feira que inicia a semana da data. Tipo date. E a chave de agrupamento semanal, porque rotulos de ano e numero de semana sao ambiguos na virada do ano.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.ano_semana IS 'Rotulo legivel da semana, no formato aaaa-Snn. Texto. Apenas para leitura.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.trimestre IS 'Trimestre do ano. Inteiro. Dominio: 1 a 4.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.semestre IS 'Semestre do ano. Inteiro. Dominio: 1, 2.';
COMMENT ON COLUMN combustiveis.gold.dim_tempo.dia_semana IS 'Nome do dia da semana, em ingles. Texto. Dominio: Monday a Sunday.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferencia do catalogo
-- MAGIC
-- MAGIC A consulta abaixo le os comentarios gravados no Unity Catalog. Serve de evidencia de que a
-- MAGIC documentacao esta na plataforma, e de fonte para a transcricao do catalogo no README.

-- COMMAND ----------

SELECT table_schema AS camada,
       table_name AS tabela,
       column_name AS coluna,
       data_type AS tipo,
       comment AS descricao
FROM combustiveis.information_schema.columns
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name, ordinal_position;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Colunas ainda sem descricao. O resultado esperado e vazio.

-- COMMAND ----------

SELECT table_schema, table_name, column_name
FROM combustiveis.information_schema.columns
WHERE table_schema IN ('bronze', 'silver', 'gold')
  AND (comment IS NULL OR trim(comment) = '')
ORDER BY table_schema, table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Descricao das tabelas.

-- COMMAND ----------

SELECT table_schema AS camada, table_name AS tabela, comment AS descricao
FROM combustiveis.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;
