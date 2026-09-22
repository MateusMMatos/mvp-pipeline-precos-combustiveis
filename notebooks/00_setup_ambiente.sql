-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 - Preparacao do ambiente
-- MAGIC
-- MAGIC Cria a estrutura que vai receber o pipeline: um catalogo para o projeto, um schema por camada
-- MAGIC da arquitetura medalhao e um volume para os arquivos brutos da ANP.
-- MAGIC
-- MAGIC Organizacao adotada: um catalogo por projeto e as camadas como schemas. O enunciado admite um
-- MAGIC catalogo por camada; optamos pelo arranjo de um catalogo unico porque o projeto vive em um
-- MAGIC ambiente so, os nomes qualificados ficam mais curtos nas consultas entre camadas e a permissao
-- MAGIC de criar catalogos pode ser limitada na Free Edition.
-- MAGIC
-- MAGIC O volume guarda arquivos; a tabela guarda linhas e colunas. O CSV bruto entra no volume e so
-- MAGIC depois vira tabela Delta na camada bronze.

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
-- MAGIC ## Conferencia
-- MAGIC
-- MAGIC As tres consultas abaixo servem de evidencia de que a estrutura foi criada.

-- COMMAND ----------

SHOW SCHEMAS IN combustiveis;

-- COMMAND ----------

SHOW VOLUMES IN combustiveis.bronze;

-- COMMAND ----------

DESCRIBE CATALOG combustiveis;
