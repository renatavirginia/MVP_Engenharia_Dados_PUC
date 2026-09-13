# MVP de Engenharia de Dados: Risco de Ruptura de Estoque no Varejo

**Nome:** Renata Virginia<br>
**Matrícula:** 4052025002400

## 1. Contexto de Negócios e Perguntas

### Descrição do problema

Uma empresa varejista precisa garantir a disponibilidade dos produtos para atender à demanda dos consumidores. A ocorrência de níveis insuficientes de estoque pode resultar em **ruptura**, perda potencial de vendas e pior experiência para o consumidor. Ao mesmo tempo, manter estoques excessivamente elevados aumenta custos e capital imobilizado.

O objetivo deste projeto é construir um pipeline de dados capaz de organizar e transformar dados históricos de vendas e estoque para **identificar padrões associados ao risco de ruptura** e gerar informações que apoiem decisões de abastecimento.

### Dataset

- **Nome:** Retail Store Inventory Forecasting Dataset
- **Fonte:** Kaggle — Anirudh Chauhan
- **Licença:** CC0 (Public Domain)
- **Volume:** ~73.000 registros diários
- **Conteúdo:** Informações de lojas, produtos, vendas, níveis de estoque, preços, promoções, feriados e condições climáticas

### Regra de negócio — Estoque Crítico

O dataset não possui uma coluna explícita de ruptura. Foi criada a seguinte regra:

> **`flag_estoque_critico = True`** quando `dias_cobertura < 7`
>
> onde `dias_cobertura = nivel_estoque / demanda_media_7d`

**Justificativa:** O critério de 7 dias de cobertura é padrão de mercado para varejo de ciclo curto. Abaixo desse limiar, o estoque está em zona de risco iminente de ruptura antes do próximo ciclo de reabastecimento.

### Perguntas de negócio

| # | Pergunta |
|---|---|
| P1 | Quais produtos, categorias e lojas têm maior frequência de estoque crítico? |
| P2 | Há relação entre alto volume de vendas e maior risco de estoque crítico? |
| P3 | Promoções aumentam a pressão sobre o estoque e geram mais eventos críticos? |
| P4 | Feriados e períodos específicos apresentam maior pressão sobre o estoque? |
| P5 | Há relação entre preço do produto e risco de estoque crítico? *(complementar)* |

---

## 2. Carga dos Dados

O dataset foi baixado do Kaggle em formato CSV e carregado diretamente no Databricks via **Unity Catalog > Volumes**.

**Como foi feita a carga:**
1. Download manual do arquivo `retail_store_inventory.csv` do Kaggle
2. Upload para o Volume `/Volumes/workspace/default/mvp_dados/` no Databricks
3. Leitura com PySpark no notebook `01_bronze_ingestao.py`
4. Gravação como tabela Delta na camada Bronze sem nenhuma alteração nos dados

> Script de referência: [`notebooks/01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py)

*(Adicionar screenshot da tabela bronze no Unity Catalog)*

---

## 3. Modelagem e Catálogo de Dados

### Arquitetura Medallion

```
Bronze (dado bruto)  →  Silver (limpo e padronizado)  →  Gold (modelado para análise)
inventory_raw            inventory_clean                  fato_estoque_diario
                                                          dim_produto
                                                          dim_loja
                                                          dim_data
```

### Esquema Estrela (camada Gold)

*(Adicionar diagrama do modelo estrela)*

### Catálogo de Dados

#### `bronze.inventory_raw`
Dados brutos ingeridos diretamente do Kaggle, sem transformações.

| Coluna | Tipo | Descrição |
|---|---|---|
| Store ID | String | Identificador da loja |
| Product ID | String | Identificador do produto |
| Category | String | Categoria do produto |
| Region | String | Região geográfica da loja |
| Date | String | Data do registro |
| Inventory Level | Integer | Nível de estoque disponível |
| Units Sold | Integer | Unidades vendidas no dia |
| Units Ordered | Integer | Unidades pedidas/repostas no dia |
| Demand Forecast | Double | Previsão de demanda |
| Price | Double | Preço unitário do produto |
| Discount | Double | Desconto aplicado |
| Weather Condition | String | Condição climática do dia |
| Holiday/Promotion | String | Indicador de feriado ou promoção |
| Competitor Pricing | Double | Preço do concorrente |
| Seasonality | String | Estação do ano |
| ingestion_date | Timestamp | Data/hora da ingestão (metadado) |
| source_file | String | Caminho do arquivo de origem (metadado) |

#### `silver.inventory_clean`
Dados limpos e padronizados com nomes de colunas em português.

| Coluna | Tipo | Descrição |
|---|---|---|
| loja_id | String | Identificador da loja |
| produto_id | String | Identificador do produto |
| categoria | String | Categoria (padronizada em uppercase) |
| regiao | String | Região (padronizada em uppercase) |
| data | Date | Data do registro |
| nivel_estoque | Integer | Estoque disponível no dia |
| unidades_vendidas | Integer | Unidades vendidas no dia |
| unidades_pedidas | Integer | Unidades repostas no dia |
| previsao_demanda | Double | Previsão de demanda |
| preco_unitario | Double | Preço unitário |
| desconto | Double | Desconto aplicado |
| condicao_climatica | String | Condição climática |
| em_promocao | Boolean | Se havia promoção ativa no dia |
| feriado | Boolean | Se era feriado |
| preco_concorrente | Double | Preço do concorrente |
| sazonalidade | String | Estação do ano |

#### `gold.dim_produto`

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_produto | Integer | Chave surrogate | Gerado sequencialmente |
| produto_id_orig | String | ID original do dataset | — |
| nome_produto | String | Nome/código do produto | — |
| categoria | String | Categoria do produto | Ex: Electronics, Clothing |

#### `gold.dim_loja`

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_loja | Integer | Chave surrogate | Gerado sequencialmente |
| loja_id_orig | String | ID original do dataset | — |
| nome_loja | String | Nome/código da loja | — |
| regiao | String | Região geográfica | Ex: North, South, East, West |

#### `gold.dim_data`

| Coluna | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_data | Integer | Chave no formato YYYYMMDD | Ex: 20230115 |
| data_completa | Date | Data completa | — |
| ano | Integer | Ano | — |
| mes | Integer | Mês numérico | 1–12 |
| nome_mes | String | Nome do mês | January–December |
| dia_semana | Integer | Dia da semana | 1 (Dom) – 7 (Sab) |
| trimestre | Integer | Trimestre | 1–4 |
| feriado | Boolean | Se é feriado | true/false |

#### `gold.fato_estoque_diario`

| Coluna | Tipo | Descrição | Linhagem |
|---|---|---|---|
| id_data | Integer | FK → dim_data | Silver: data |
| id_produto | Integer | FK → dim_produto | Silver: produto_id |
| id_loja | Integer | FK → dim_loja | Silver: loja_id |
| unidades_vendidas | Integer | Quantidade vendida no dia | Silver: unidades_vendidas |
| unidades_pedidas | Integer | Quantidade reposta no dia | Silver: unidades_pedidas |
| nivel_estoque | Integer | Estoque disponível | Silver: nivel_estoque |
| preco_unitario | Double | Preço do produto | Silver: preco_unitario |
| desconto | Double | Desconto aplicado | Silver: desconto |
| em_promocao | Boolean | Se havia promoção | Silver: em_promocao |
| feriado | Boolean | Se era feriado | Silver: feriado |
| condicao_climatica | String | Condição do tempo | Silver: condicao_climatica |
| sazonalidade | String | Estação do ano | Silver: sazonalidade |
| previsao_demanda | Double | Previsão de demanda | Silver: previsao_demanda |
| preco_concorrente | Double | Preço do concorrente | Silver: preco_concorrente |
| demanda_media_7d | Double | Média móvel 7d de vendas | **Calculado** via window function |
| dias_cobertura | Double | Estoque / demanda_media_7d | **Calculado**: nivel_estoque / demanda_media_7d |
| flag_estoque_critico | Boolean | Indicador de risco de ruptura | **Calculado**: dias_cobertura < 7 |

*(Adicionar screenshots do Unity Catalog com as tabelas e descrições)*

---

## 4. Pipeline de Dados / ETL

O pipeline foi organizado em notebooks separados por camada, seguindo a Arquitetura Medallion:

| Notebook | Camada | Responsabilidade |
|---|---|---|
| `01_bronze_ingestao.py` | Bronze | Leitura do CSV bruto e gravação sem transformações |
| `02_silver_transformacao.py` | Silver | Limpeza, padronização e remoção de duplicatas |
| `03_gold_modelagem.py` | Gold | Criação do Esquema Estrela e aplicação da regra de ruptura |

### Transformações realizadas (Silver)

| Transformação | Decisão |
|---|---|
| Renomeação de colunas | Nomes padronizados em português |
| Conversão de tipos | `data` → DateType, numéricos → Int/Double |
| Separação feriado/promoção | Criadas colunas booleanas `em_promocao` e `feriado` |
| Padronização de strings | Trim + uppercase em categorias e regiões |
| Duplicatas | Removidas por chave `loja_id + produto_id + data` |
| Nulos essenciais | Linhas com `nivel_estoque` ou `unidades_vendidas` nulos removidas |
| Nulos secundários | Substituídos por valor padrão (0 ou "NAO_INFORMADO") |
| Valores impossíveis | Estoque/vendas negativos e preço zero removidos |

### Métricas calculadas (Gold)

| Métrica | Fórmula | Ferramenta |
|---|---|---|
| `demanda_media_7d` | Média móvel de 7 dias de `unidades_vendidas` por produto+loja | PySpark Window Function |
| `dias_cobertura` | `nivel_estoque / demanda_media_7d` | PySpark |
| `flag_estoque_critico` | `dias_cobertura < 7` | PySpark |

*(Adicionar screenshots das tabelas salvas no Databricks)*

---

## 5. Qualidade de Dados

A verificação de qualidade foi realizada no notebook `04_qualidade_dados.py`, cobrindo as seguintes dimensões:

| Dimensão | O que foi verificado | Resultado |
|---|---|---|
| Completude | % de nulos por coluna em todas as camadas | *(preencher após execução)* |
| Unicidade | Duplicatas na chave `loja_id + produto_id + data` | *(preencher após execução)* |
| Consistência | Formato de datas, domínio de categorias e regiões | *(preencher após execução)* |
| Acurácia | Valores negativos em estoque, vendas e preço | *(preencher após execução)* |
| Outliers | Percentis de estoque, vendas e dias de cobertura | *(preencher após execução)* |

*(Adicionar screenshots do relatório de qualidade)*

---

## 6. Análise de Dados

As análises foram realizadas no notebook `05_analise_negocio.py` com SQL e Python (Matplotlib/Seaborn).

*(Adicionar screenshots dos gráficos e resultados de cada pergunta após execução no Databricks)*

### P1 — Produtos e lojas com maior risco
*(preencher com resultado e interpretação após execução)*

### P2 — Demanda e risco de ruptura
*(preencher com resultado e interpretação após execução)*

### P3 — Impacto das promoções
*(preencher com resultado e interpretação após execução)*

### P4 — Sazonalidade e feriados
*(preencher com resultado e interpretação após execução)*

### P5 — Preço e risco (complementar)
*(preencher após verificar variação de preços nos dados)*

---

## 7. Autoavaliação

*(Preencher ao finalizar o projeto)*

### O que foi atingido
- [ ] Pipeline completo Bronze → Silver → Gold implementado
- [ ] Regra de negócio para estoque crítico documentada e justificada
- [ ] Esquema Estrela com tabela fato e 3 dimensões
- [ ] Catálogo de dados completo no Unity Catalog
- [ ] Verificação de qualidade de dados em todas as dimensões
- [ ] 5 perguntas de negócio respondidas com visualizações

### Dificuldades encontradas
*(descrever os principais desafios durante a execução)*

### Trabalhos futuros
- Integrar dados mais recentes (atualizações do dataset)
- Criar alertas automáticos para produtos com `dias_cobertura < 7`
- Explorar modelos de previsão de demanda para antecipar rupturas
- Construir dashboard interativo no Databricks SQL

---

### Ferramentas e Tecnologias

- **Databricks Free Edition** — plataforma de processamento na nuvem (Apache Spark)
- **Delta Lake** — formato de armazenamento com suporte a ACID e versionamento
- **Unity Catalog** — catálogo centralizado de dados e controle de acesso
- **PySpark** — processamento distribuído dos dados
- **Python (Pandas, Matplotlib, Seaborn)** — análise exploratória e visualizações
- **SQL** — consultas analíticas sobre as tabelas Gold
