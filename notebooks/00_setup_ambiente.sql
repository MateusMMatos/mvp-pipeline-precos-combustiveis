-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Preparação do ambiente
-- MAGIC
-- MAGIC Cria a estrutura que recebe o pipeline: um catálogo para o projeto, um schema para cada camada da
-- MAGIC arquitetura medalhão e um volume para os arquivos brutos da ANP.
-- MAGIC
-- MAGIC O enunciado sugere um catálogo por camada. Foi adotado um catálogo único, com as camadas como
-- MAGIC schemas, porque o projeto está em um único ambiente e os nomes qualificados ficam mais curtos nas
-- MAGIC consultas entre camadas.
-- MAGIC
-- MAGIC O volume armazena arquivos, e as tabelas armazenam dados em linhas e colunas. Os CSV brutos ficam no
-- MAGIC volume e só depois são carregados como tabela Delta na camada bronze.

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS combustiveis
COMMENT 'MVP de engenharia de dados: precos de combustiveis da ANP para uma politica de abastecimento de frota.';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS combustiveis.bronze
COMMENT 'Camada bronze: dado como veio da ANP, sem limpeza, com metadados de controle de ingestao.';

CREATE SCHEMA IF NOT EXISTS combustiveis.silver
COMMENT 'Camada silver: dado tipado, padronizado e sem duplicatas.';

CREATE SCHEMA IF NOT EXISTS combustiveis.gold
COMMENT 'Camada gold: modelo estrela com a fato de precos e as dimensoes usadas nas analises.';

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS combustiveis.bronze.arquivos_anp
COMMENT 'Arquivos CSV originais da Serie Historica de Precos de Combustiveis da ANP, como baixados da fonte.';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Conferência
-- MAGIC
-- MAGIC As três consultas a seguir confirmam que a estrutura foi criada.

-- COMMAND ----------

SHOW SCHEMAS IN combustiveis;

-- COMMAND ----------

SHOW VOLUMES IN combustiveis.bronze;

-- COMMAND ----------

DESCRIBE CATALOG combustiveis;
