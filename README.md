# MVP de Engenharia de Dados: preços de combustíveis para uma política de abastecimento de frota

Pipeline de dados de ponta a ponta construído no Databricks sobre a Série Histórica de Preços de
Combustíveis da ANP. O fluxo vai do arquivo bruto publicado pela agência até as tabelas de um esquema
estrela usadas para responder às perguntas de negócio listadas abaixo.

Sprint 3 (Engenharia de Dados) da pós-graduação em Ciência de Dados e Analytics. Trabalho individual.

## Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

### Problema

Uma empresa com frota própria, formada por carros de vendedores externos e caminhões de entrega, não
tem critério de abastecimento. Cada motorista escolhe o posto e o combustível, e o reembolso é igual
em todo o país. Combustível é o segundo maior custo da operação, atrás apenas de pessoal.

A área de Compras precisa decidir, com base em dados, onde abastecer, com qual combustível e em que
tipo de posto, além de revisar o valor de reembolso por estado e projetar o orçamento do próximo ano.

### Perguntas

| # | Pergunta | Decisão que ela apoia |
|---|---|---|
| P1 | Como o preço médio de cada combustível evoluiu mês a mês entre julho de 2025 e junho de 2026? | Revisão do orçamento e do valor de reembolso |
| P2 | Quais estados têm a gasolina comum e o diesel S10 mais caros e mais baratos, e qual a diferença percentual entre os extremos? | Reembolso por estado e escolha de onde abastecer em viagem longa |
| P3 | Em quais estados compensa abastecer os carros flex com etanol, considerando a referência de 70% do preço da gasolina, e isso muda ao longo do ano? | Política de combustível por estado |
| P4 | Postos bandeirados cobram mais que postos de bandeira branca? Qual a diferença, e ela se mantém em todas as regiões? | Autorizar ou não o abastecimento em bandeira branca |
| P5 | Dentro do mesmo município e na mesma semana, qual a diferença entre o posto mais barato e o mais caro? Em quais municípios essa dispersão é maior? | Onde vale negociar convênio com postos |

A referência de 70% na P3 vem do rendimento do etanol, que entrega cerca de 70% do que a gasolina
entrega por litro. Acima dessa proporção, o litro mais barato sai mais caro por quilômetro rodado.

Posto bandeirado é o que exibe a marca de uma distribuidora e só pode vender o combustível dela. Posto
de bandeira branca não exibe marca e compra de qualquer distribuidora. As duas definições são do
dicionário de metadados da ANP.

### Granularidade e frequência de atualização

O menor nível do dado é um preço, de um combustível, em um posto, em uma data de coleta. As tabelas
finais guardam esse nível, e semana, mês, município, estado e região saem por agregação.

A pesquisa da ANP é semanal, então processamento em tempo real não se justifica. Neste MVP a carga é
em lote, a partir dos arquivos semestrais. Em produção, o mesmo pipeline rodaria em carga semanal ou
mensal, acrescentando os arquivos novos.

### Contexto dos dados brutos

| Item | Descrição |
|---|---|
| Fonte | ANP / SDC (Superintendência de Defesa da Concorrência), Levantamento de Preços de Combustíveis |
| Como é coletado | Pesquisa semanal presencial de preço ao consumidor, executada por empresa contratada, em amostra de postos revendedores |
| Base legal | Lei 9.478/1997 (Lei do Petróleo), artigo 8º |
| Período usado | Julho de 2025 a junho de 2026 |
| Volume | 806.626 registros, em dois arquivos CSV (63,7 MB e 72,1 MB) |
| Cobertura | 27 unidades da federação, 417 pares de estado e município, 7.963 postos no primeiro semestre de 2026 |
| Produtos | Gasolina comum, gasolina aditivada, etanol hidratado, diesel, diesel S10 e GNV |

A pesquisa é amostral, não um censo de postos. As conclusões valem para os municípios pesquisados.

Não há dados pessoais. Os registros identificam pessoas jurídicas (CNPJ, razão social e endereço
comercial do posto) e são de publicação obrigatória por lei, o que dispensa anonimização.

### Estrutura dos dados brutos

Cada arquivo é uma tabela única, no formato de uma linha por preço coletado, com 16 colunas e sem
arquivos relacionados. Separador ponto e vírgula, codificação UTF-8 com BOM, quebra de linha CRLF,
decimal com vírgula e data no formato dd/mm/aaaa.

| Coluna | Conteúdo |
|---|---|
| Regiao - Sigla, Estado - Sigla, Municipio | Localização do posto |
| Revenda, CNPJ da Revenda | Identificação do posto |
| Nome da Rua, Numero Rua, Complemento, Bairro, Cep | Endereço do posto |
| Produto | Combustível pesquisado |
| Data da Coleta | Data da pesquisa em campo |
| Valor de Venda | Preço ao consumidor, medida principal da análise |
| Valor de Compra | Preço de distribuição, descontinuado desde agosto de 2020 |
| Unidade de Medida | R$/litro para os combustíveis líquidos, R$/m³ para o GNV |
| Bandeira | Marca da distribuidora exibida pelo posto, ou BRANCA |

As chaves usadas na modelagem são o CNPJ da revenda para identificar o posto, o par estado e município
para a localidade, e o produto e a data da coleta para o restante.

### Licença dos dados

A ANP não nomeia uma licença específica na página do conjunto. Os dados são publicados sob a Política
de Dados Abertos do Executivo federal, instituída pelo Decreto 8.777/2016, cujo artigo 2º, inciso III,
define dado aberto como aquele disponibilizado "sob licença aberta que permita sua livre utilização,
consumo ou cruzamento, limitando-se a creditar a autoria ou a fonte".

O uso, a transformação e o cruzamento dos dados estão liberados, com citação obrigatória da fonte.

O rodapé do site da ANP exibe a licença Creative Commons Atribuição-SemDerivações 3.0, que proíbe
derivações. Essa licença cobre o conteúdo editorial do site, não os conjuntos publicados como dados
abertos, que seguem o decreto citado acima.

Fonte: ANP, Série Histórica de Preços de Combustíveis.
https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis

## Carga dos Dados (Etapa 4.2)

Em construção.

## Modelagem e Catálogo de Dados (Etapa 4.3)

Em construção.

## Pipeline de Dados (Etapa 4.4)

Em construção.

## Qualidade de Dados (Etapa 4.5)

Em construção.

## Análise de Dados (Etapa 4.5)

Em construção.

## Autoavaliação

Em construção.
