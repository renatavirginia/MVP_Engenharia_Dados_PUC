# MVP de Engenharia de Dados: Risco de Ruptura de Estoque no Varejo

**Nome:** Renata Virginia &nbsp;|&nbsp; **Matrícula:** 4052025002400  
**Plataforma:** Databricks Free Edition &nbsp;|&nbsp; **Dataset:** Retail Store Inventory Forecasting (Kaggle, CC0)

---

> **Relatório completo:**
> [`relatorio_mvp.pdf`](relatorio_mvp.pdf) &nbsp;·&nbsp;
> [`relatorio_mvp.html`](https://renatavirginia.github.io/MVP_Engenharia_Dados_PUC/relatorio_mvp.html)
>
> **Execução do pipeline no Databricks (com outputs):**
> [`pipeline_completo.html`](https://renatavirginia.github.io/MVP_Engenharia_Dados_PUC/pipeline_completo.html)

---

## Sumário

- [Contexto de Negócios e Perguntas (Etapa 2 e 4.1)](#contexto-de-negócios-e-perguntas-etapa-2-e-41)
- [Carga dos Dados (Etapa 4.2)](#carga-dos-dados-etapa-42)
- [Modelagem e Catálogo de Dados (Etapa 4.3)](#modelagem-e-catálogo-de-dados-etapa-43)
- [Pipeline de Dados (Etapa 4.4)](#pipeline-de-dados-etapa-44)
- [Qualidade de Dados (Etapa 4.5)](#qualidade-de-dados-etapa-45)
- [Análise de Dados (Etapa 4.5)](#análise-de-dados-etapa-45)
- [Autoavaliação](#7-autoavaliação)
- [Tecnologias](#tecnologias)

---

## Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

Uma empresa varejista precisa garantir a disponibilidade dos produtos para atender à demanda dos consumidores. A ocorrência de níveis insuficientes de estoque pode resultar em **ruptura**, perda potencial de vendas e pior experiência para o consumidor. Ao mesmo tempo, manter estoques excessivamente elevados aumenta custos e capital imobilizado.

O objetivo deste projeto é construir um pipeline de dados capaz de organizar e transformar dados históricos de vendas e estoque para **identificar padrões associados ao risco de ruptura** e gerar informações que apoiem decisões de abastecimento.

<details>
<summary><strong>Dataset</strong></summary>

- **Nome:** Retail Store Inventory Forecasting Dataset
- **Fonte:** [Kaggle — Anirudh Chauhan](https://www.kaggle.com/datasets/anirudhchauhan/retail-store-inventory-forecasting-dataset)
- **Licença:** CC0 (Public Domain) — uso livre, sem necessidade de atribuição
- **Volume:** 73.100 registros diários | **Período:** 2022-01-01 a 2024-01-01
- **Natureza:** dataset **sintético** — valores gerados artificialmente para fins de previsão de demanda; diferenças entre dimensões tendem a ser pequenas
- **Conteúdo:** 5 lojas (S001–S005), 20 produtos (P0001–P0020), vendas, níveis de estoque, preços, promoções, feriados e condições climáticas

</details>

<details>
<summary><strong>Regra de negócio: Estoque Crítico</strong></summary>

O dataset não possui uma coluna explícita de ruptura. Foi criada a seguinte regra:

> **`flag_estoque_critico = True`** quando `dias_cobertura < p25(dias_cobertura)`
>
> onde `dias_cobertura = nivel_estoque / demanda_media_7d`

**Justificativa:** O limiar é o percentil 25 da distribuição real de `dias_cobertura`, sinalizando os 25% de registros com pior cobertura relativa. Um limiar fixo de 7 dias marcaria ~99,5% dos registros como críticos neste dataset (cobertura média real ≈ 2 dias), tornando a flag inútil como discriminador analítico.

</details>

<details>
<summary><strong>Perguntas de negócio</strong></summary>

| # | Pergunta |
|---|---|
| P1 | Quais produtos, categorias e lojas têm maior frequência de estoque crítico? |
| P2 | Há relação entre alto volume de vendas e maior risco de estoque crítico? |
| P3 | Dias com feriado/promoção ativo geram maior pressão sobre o estoque? |
| P4 | Períodos específicos apresentam maior pressão sobre o estoque? |
| P5 | Há relação entre preço do produto e risco de estoque crítico? *(complementar)* |

</details>

---

## Carga dos Dados (Etapa 4.2)

> Notebook: [`notebooks/01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py)

O dataset está disponível como tabela Delta gerenciada no Unity Catalog do Databricks (`workspace.default.retail_store_inventory`), carregada previamente a partir do arquivo CSV original do Kaggle.

<details>
<summary><strong>Como foi feita a carga</strong></summary>

1. Download do arquivo `retail_store_inventory.csv` do Kaggle (autor: Anirudh Chauhan, licença CC0)
2. Ingestão como tabela Delta no Unity Catalog: `workspace.default.retail_store_inventory`
3. Leitura com PySpark via `spark.table()` no notebook `01_bronze_ingestao.py`
4. Renomeação das colunas para snake_case (necessário para compatibilidade com Delta Lake)
5. Adição dos metadados `ingestion_date` e `source_table`
6. Gravação como tabela Delta na camada Bronze: **73.100 registros**

</details>

<details>
<summary><strong>Evidências da tabela de origem no Unity Catalog</strong></summary>

**Tabela de origem — `workspace.default.retail_store_inventory` (colunas originais)**

![Tabela de origem - colunas](images/uc_origem.png)

**Detalhes da tabela — tipo Managed, formato Delta, criada em 30/08/2026**

![Tabela de origem - detalhes](images/uc_origem_detalhes.png)

</details>

---

## Modelagem e Catálogo de Dados (Etapa 4.3)

> Notebook: [`notebooks/03_gold_modelagem.py`](notebooks/03_gold_modelagem.py)

<details>
<summary><strong>Arquitetura Medallion</strong></summary>

```
Bronze (dado bruto)  →  Silver (limpo e padronizado)  →  Gold (modelado para análise)
inventory_raw            inventory_clean                  fato_estoque_diario
                                                          dim_produto
                                                          dim_loja
                                                          dim_data
```

</details>

<details>
<summary><strong>Esquema Estrela (camada Gold)</strong></summary>

```
                        +------------------+
                        |    dim_data      |
                        +------------------+
                        | id_data (PK)     |
                        | data_completa    |
                        | ano / mes        |
                        | trimestre        |
                        | dia_semana       |
                        | nome_mes         |
                        +--------+---------+
                                 |
+----------------+    +----------+-------------------+    +--------------+
|  dim_produto   |    |    fato_estoque_diario        |    |  dim_loja    |
+----------------+    +------------------------------+    +--------------+
| id_produto (PK)|<---| id_produto (FK)              |    | id_loja (PK) |
| produto_id_orig|    | id_loja    (FK) ------------>|--->| loja_id_orig |
| nome_produto   |    | id_data    (FK)              |    | nome_loja    |
+----------------+    | categoria  (*)               |    +--------------+
                      | regiao     (*)               |
                      | unidades_vendidas            |
                      | nivel_estoque                |
                      | preco_unitario               |
                      | feriado_ou_promocao_ativo    |
                      | demanda_media_7d  (calculado)|
                      | dias_cobertura    (calculado)|
                      | flag_estoque_critico (calc.) |
                      +------------------------------+

(*) No dataset, o mesmo produto aparece com categorias diferentes
    e a mesma loja com regiões diferentes por registro. Por isso,
    categoria e regiao são atributos da fato (variam por linha),
    não das dimensões.
```

</details>

<details>
<summary><strong>Catálogo de Dados — bronze.inventory_raw</strong></summary>

73.100 registros. Dado bruto preservado exatamente como veio da fonte. Colunas renomeadas para snake_case (necessário para compatibilidade com Delta Lake) e dois metadados de controle adicionados.

| Coluna | Tipo | Domínio | Descrição | Linhagem |
|---|---|---|---|---|
| store_id | String | "S001" – "S005" (5 lojas) | Identificador da loja | Fonte: `workspace.default.retail_store_inventory` — coluna `Store ID` |
| product_id | String | "P0001" – "P0020" (20 produtos) | Identificador do produto | Fonte: `workspace.default.retail_store_inventory` — coluna `Product ID` |
| category | String | Texto livre — valores brutos da fonte | Categoria do produto | Fonte: coluna `Category` |
| region | String | Texto livre — valores brutos da fonte | Região geográfica da loja | Fonte: coluna `Region` |
| date | String | Formato "YYYY-MM-DD" | Data do registro | Fonte: coluna `Date` |
| inventory_level | Integer | Sem restrição — dado bruto | Nível de estoque disponível | Fonte: coluna `Inventory Level` |
| units_sold | Integer | Sem restrição — dado bruto | Unidades vendidas no dia | Fonte: coluna `Units Sold` |
| units_ordered | Integer | Sem restrição — dado bruto | Unidades pedidas/repostas no dia | Fonte: coluna `Units Ordered` |
| demand_forecast | Double | Sem restrição — dado bruto | Previsão de demanda | Fonte: coluna `Demand Forecast` |
| price | Double | Sem restrição — dado bruto | Preço unitário do produto | Fonte: coluna `Price` |
| discount | Integer | Sem restrição — dado bruto | Desconto aplicado (valor percentual inteiro) | Fonte: coluna `Discount` — tipo inferido pelo Spark como bigint |
| weather_condition | String | Texto livre — valores brutos da fonte | Condição climática do dia | Fonte: coluna `Weather Condition` |
| holiday_promotion | Integer | 0 ou 1 — dado bruto | Indicador de feriado ou promoção | Fonte: coluna `Holiday/Promotion` — tipo inferido pelo Spark como bigint |
| competitor_pricing | Double | Sem restrição — dado bruto | Preço do concorrente | Fonte: coluna `Competitor Pricing` |
| seasonality | String | Texto livre — valores brutos da fonte | Estação do ano | Fonte: coluna `Seasonality` |
| ingestion_date | Timestamp | Data/hora da execução do pipeline | Data/hora da ingestão (metadado) | **Calculado**: `current_timestamp()` no notebook `01_bronze_ingestao.py` |
| source_table | String | Sempre "workspace.default.retail_store_inventory" | Tabela de origem no Unity Catalog (metadado) | **Calculado**: valor literal adicionado no notebook `01_bronze_ingestao.py` |

</details>

<details>
<summary><strong>Catálogo de Dados — silver.inventory_clean</strong></summary>

73.100 registros. Dados limpos, tipados e padronizados. Nomes de colunas em português. Registros com valores impossíveis e nulos em colunas essenciais foram removidos; nulos em colunas secundárias foram substituídos por valores padrão.

| Coluna | Tipo | Domínio | Descrição | Linhagem |
|---|---|---|---|---|
| loja_id | String | "S001" – "S005" (5 lojas únicas) | Identificador da loja | Bronze: `store_id` — renomeado para português |
| produto_id | String | "P0001" – "P0020" (20 produtos únicos) | Identificador do produto | Bronze: `product_id` — renomeado para português |
| categoria | String | CLOTHING, ELECTRONICS, FURNITURE, GROCERIES, TOYS | Categoria do produto (uppercase) | Bronze: `category` — padronizado para uppercase via `F.upper()` |
| regiao | String | EAST, NORTH, SOUTH, WEST | Região geográfica da loja (uppercase) | Bronze: `region` — padronizado para uppercase via `F.upper()` |
| data | Date | Período do dataset — mín./máx. verificados em `04_qualidade_dados` | Data do registro | Bronze: `date` — convertido de String para DateType |
| nivel_estoque | Integer | ≥ 0 (registros com valor negativo removidos) | Estoque disponível no dia | Bronze: `inventory_level` — convertido para IntegerType |
| unidades_vendidas | Integer | ≥ 0 (registros com valor negativo removidos) | Unidades vendidas no dia | Bronze: `units_sold` — convertido para IntegerType |
| unidades_pedidas | Integer | ≥ 0 | Unidades pedidas/repostas no dia | Bronze: `units_ordered` — convertido para IntegerType |
| previsao_demanda | Double | −9,99 a ~518 (dataset sintético; nulos → 0; valores negativos mantidos — ver Qualidade) | Previsão de demanda para o dia | Bronze: `demand_forecast` — convertido para DoubleType; nulos → 0 |
| preco_unitario | Double | > 0 (registros com preço ≤ 0 removidos) | Preço unitário do produto | Bronze: `price` — convertido para DoubleType |
| desconto | Double | ≥ 0 (nulos substituídos por 0) | Desconto aplicado no dia | Bronze: `discount` — convertido para DoubleType; nulos → 0 |
| condicao_climatica | String | Sunny, Rainy, Cloudy, Snowy, "NAO_INFORMADO" (para nulos) | Condição climática do dia | Bronze: `weather_condition` — nulos → "NAO_INFORMADO" |
| feriado_ou_promocao | Integer | 0 ou 1 — não distingue feriado de promoção | Indicador combinado original (coluna original preservada) | Bronze: `holiday_promotion` — convertido para Integer |
| feriado_ou_promocao_ativo | Boolean | True / False | Se havia feriado ou promoção no dia | **Calculado**: `feriado_ou_promocao.cast("integer") == 1` |
| preco_concorrente | Double | ≥ 0 (nulos substituídos por 0) | Preço praticado pelo concorrente | Bronze: `competitor_pricing` — convertido para DoubleType; nulos → 0 |
| sazonalidade | String | AUTUMN, SPRING, SUMMER, WINTER | Estação do ano (uppercase) | Bronze: `seasonality` — padronizado para uppercase via `F.upper()` |

</details>

<details>
<summary><strong>Catálogo de Dados — gold (dimensões e fato)</strong></summary>

Camada analítica no formato de Esquema Estrela. Tabelas gravadas em formato Delta Lake no schema `workspace.gold`.

| Tabela | Tipo | Registros |
|---|---|---|
| `dim_produto` | Dimensão | 20 |
| `dim_loja` | Dimensão | 5 |
| `dim_data` | Dimensão | 731 |
| `fato_estoque_diario` | Fato | 73.100 |

---

**gold.dim_produto** *(dimensão lean — categoria fica na fato pois varia por registro)*

| Coluna | Tipo | Domínio | Descrição | Linhagem |
|---|---|---|---|---|
| id_produto | Integer | 1 – 20 (surrogate key sequencial) | Chave primária (PK) | Gerado: `row_number()` sobre `Window.orderBy("produto_id")` |
| produto_id_orig | String | "P0001" – "P0020" (20 produtos únicos) | Código original do produto | Silver: `produto_id` |
| nome_produto | String | "P0001" – "P0020" | Nome do produto (igual ao código neste dataset) | Silver: `produto_id` |

---

**gold.dim_loja** *(dimensão lean — regiao fica na fato pois varia por registro)*

| Coluna | Tipo | Domínio | Descrição | Linhagem |
|---|---|---|---|---|
| id_loja | Integer | 1 – 5 (surrogate key sequencial) | Chave primária (PK) | Gerado: `row_number()` sobre `Window.orderBy("loja_id")` |
| loja_id_orig | String | "S001" – "S005" (5 lojas únicas) | Código original da loja | Silver: `loja_id` |
| nome_loja | String | "S001" – "S005" | Nome da loja (igual ao código neste dataset) | Silver: `loja_id` |

---

**gold.dim_data**

| Coluna | Tipo | Domínio | Descrição | Linhagem |
|---|---|---|---|---|
| id_data | Integer | Formato yyyyMMdd (ex: 20230101) | Chave primária (PK) | Gerado: `date_format(data, "yyyyMMdd")` |
| data_completa | Date | Período do dataset | Data completa | Silver: data |
| ano | Integer | Anos presentes no dataset | Ano | Extraído de data_completa |
| mes | Integer | 1 – 12 | Número do mês | Extraído de data_completa |
| nome_mes | String | January – December | Nome do mês por extenso | Extraído de data_completa |
| dia_semana | Integer | 1 (domingo) – 7 (sábado) | Dia da semana (padrão Spark) | Extraído de data_completa |
| trimestre | Integer | 1 – 4 | Trimestre do ano | Extraído de data_completa |

---

**gold.fato_estoque_diario**

| Coluna | Tipo | Domínio | Descrição | Linhagem |
|---|---|---|---|---|
| id_data | Integer | Formato yyyyMMdd (FK → dim_data) | Referência à dimensão data | Silver: `data` |
| id_produto | Integer | 1 – 20 (FK → dim_produto) | Referência à dimensão produto | Silver: `produto_id` via join com dim_produto |
| id_loja | Integer | 1 – 5 (FK → dim_loja) | Referência à dimensão loja | Silver: `loja_id` via join com dim_loja |
| categoria | String | CLOTHING, ELECTRONICS, FURNITURE, GROCERIES, TOYS | Categoria do produto no dia | Silver: `categoria` — varia por registro (mesmo produto pode ter categorias distintas) |
| regiao | String | EAST, NORTH, SOUTH, WEST | Região geográfica da loja no dia | Silver: `regiao` — varia por registro (mesma loja pode ter regiões distintas) |
| nivel_estoque | Integer | ≥ 0 | Estoque disponível no dia | Silver: `nivel_estoque` |
| unidades_vendidas | Integer | ≥ 0 | Quantidade vendida no dia | Silver: unidades_vendidas |
| unidades_pedidas | Integer | ≥ 0 | Quantidade pedida/reposta no dia | Silver: unidades_pedidas |
| preco_unitario | Double | > 0 | Preço unitário do produto | Silver: preco_unitario |
| desconto | Double | ≥ 0 | Desconto aplicado no dia | Silver: desconto |
| feriado_ou_promocao_ativo | Boolean | True / False | Se havia feriado ou promoção ativo no dia | Silver: feriado_ou_promocao_ativo |
| condicao_climatica | String | Sunny, Rainy, Cloudy, Snowy, "NAO_INFORMADO" | Condição climática | Silver: condicao_climatica |
| sazonalidade | String | AUTUMN, SPRING, SUMMER, WINTER | Estação do ano | Silver: `sazonalidade` |
| previsao_demanda | Double | −9,99 a ~518 (inclui valores negativos — ver Qualidade) | Previsão de demanda para o dia | Silver: `previsao_demanda` |
| preco_concorrente | Double | ≥ 0 | Preço praticado pelo concorrente | Silver: preco_concorrente |
| demanda_media_7d | Double | ≥ 0 | Média móvel de 7 dias de `unidades_vendidas` por produto+loja | **Calculado**: `avg(unidades_vendidas)` com window de 7 dias |
| dias_cobertura | Double | 0 a 999 (999 indica produto sem demanda registrada) | Dias até esgotamento do estoque | **Calculado**: `nivel_estoque / demanda_media_7d` |
| flag_estoque_critico | Boolean | True para os 25% de registros com menor cobertura relativa | Indicador de risco de ruptura | **Calculado**: `dias_cobertura < p25(dias_cobertura)` |

</details>

<details>
<summary><strong>Evidências no Unity Catalog (Databricks)</strong></summary>

As tabelas foram criadas e registradas no Unity Catalog do Databricks Free Edition sob o catálogo `workspace`. As capturas de tela abaixo evidenciam a estrutura de schemas e colunas registradas na plataforma.

**Schema Bronze — `workspace.bronze` → tabela `inventory_raw`**

![Unity Catalog - Bronze (colunas 1/2)](images/uc_bronze.png)
![Unity Catalog - Bronze (colunas 2/2)](images/uc_bronze_2.png)

**Schema Silver — `workspace.silver` → tabela `inventory_clean`**

![Unity Catalog - Silver (colunas 1/2)](images/uc_silver.png)
![Unity Catalog - Silver (colunas 2/2)](images/uc_silver_2.png)

**Schema Gold — `workspace.gold` → tabelas `dim_produto`, `dim_loja`, `dim_data`, `fato_estoque_diario`**

![Unity Catalog - Gold](images/uc_gold.png)

</details>

---

## Pipeline de Dados (Etapa 4.4)

O pipeline foi organizado em notebooks separados por camada, seguindo a Arquitetura Medallion. Cada notebook é independente e pode ser executado isoladamente após o anterior ter sido concluído.

| Notebook | Camada | Responsabilidade |
|---|---|---|
| [`00_pipeline_completo.py`](notebooks/00_pipeline_completo.py) | Todas | Executa todas as etapas em sequência |
| [`01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py) | Bronze | Leitura da tabela de origem e gravação sem transformações |
| [`02_silver_transformacao.py`](notebooks/02_silver_transformacao.py) | Silver | Limpeza, padronização e remoção de duplicatas |
| [`03_gold_modelagem.py`](notebooks/03_gold_modelagem.py) | Gold | Criação do Esquema Estrela e aplicação da regra de ruptura |
| [`04_qualidade_dados.py`](notebooks/04_qualidade_dados.py) | Bronze / Silver / Gold | Verificação de qualidade nas três camadas |
| [`05_analise_negocio.py`](notebooks/05_analise_negocio.py) | Gold | Resposta às 5 perguntas de negócio com visualizações |

<details>
<summary><strong>Transformações realizadas (Silver →</strong> <code>02_silver_transformacao.py</code><strong>)</strong></summary>

| Transformação | Decisão |
|---|---|
| Renomeação de colunas | Nomes padronizados em português |
| Conversão de tipos | `data` → DateType, numéricos → Int/Double |
| Indicador feriado/promoção | Criado indicador único `feriado_ou_promocao_ativo` (coluna original não distingue feriado de promoção) |
| Padronização de strings | Trim + uppercase em categorias e regiões |
| Duplicatas | Removidas por chave `loja_id + produto_id + data` |
| Nulos essenciais | Linhas com `nivel_estoque` ou `unidades_vendidas` nulos removidas |
| Nulos secundários | Substituídos por valor padrão (0 ou "NAO_INFORMADO") |
| Valores impossíveis | Estoque/vendas negativos e preço zero removidos |

</details>

<details>
<summary><strong>Métricas calculadas (Gold →</strong> <code>03_gold_modelagem.py</code><strong>)</strong></summary>

| Métrica | Fórmula | Ferramenta |
|---|---|---|
| `demanda_media_7d` | Média móvel de 7 dias de `unidades_vendidas` por produto+loja | PySpark Window Function |
| `dias_cobertura` | `nivel_estoque / demanda_media_7d` | PySpark |
| `flag_estoque_critico` | `dias_cobertura < p25(dias_cobertura)` (limiar adaptativo) | PySpark |

</details>

<details>
<summary><strong>Evidências de persistência das tabelas na plataforma de nuvem</strong></summary>

As tabelas foram gravadas no formato **Delta Lake** dentro do Unity Catalog do **Databricks Free Edition**, conforme confirmado pelos outputs de execução dos notebooks. O arquivo [`pipeline_completo.html`](pipeline_completo.html) contém o log completo de execução de todas as etapas, incluindo as mensagens de confirmação de gravação de cada tabela:

```
Tabela workspace.bronze.inventory_raw gravada com 73.100 registros.
Tabela workspace.silver.inventory_clean gravada com 73.100 registros.
dim_produto: 20 registros
dim_loja: 5 registros
dim_data: 731 registros
fato_estoque_diario: 73.100 registros
```

A captura de tela abaixo mostra o Unity Catalog do Databricks com os três schemas (`bronze`, `silver`, `gold`) e suas tabelas persistidas:

![Tabelas persistidas no Databricks — schemas Bronze, Silver e Gold](images/tabelas_databricks.png)

</details>

---

## Qualidade de Dados (Etapa 4.5)

> Notebook: [`notebooks/04_qualidade_dados.py`](notebooks/04_qualidade_dados.py)

A verificação de qualidade cobriu as três camadas (Bronze, Silver e Gold) em cinco dimensões:

| Dimensão | O que foi verificado | Resultado |
|---|---|---|
| Completude | % de nulos por coluna em todas as camadas | 0 nulos em todas as colunas |
| Unicidade | Duplicatas na chave `loja_id + produto_id + data` | 0 duplicatas |
| Consistência | Formato de datas, domínio de categorias e regiões | Sem inconsistências |
| Acurácia | Valores negativos em estoque, vendas e preço | 0 registros inválidos |
| Outliers | Percentis de estoque, vendas e dias de cobertura | Distribuição esperada |

---

## Análise de Dados (Etapa 4.5)

> Notebook: [`notebooks/05_analise_negocio.py`](notebooks/05_analise_negocio.py)

<details>
<summary><strong>P1 — Quais produtos, categorias e lojas têm maior frequência de estoque crítico?</strong></summary>

![Estoque crítico por categoria](images/p1_1_categoria.png)
![Top 15 produtos críticos](images/p1_2_top15_produtos.png)
![Estoque crítico por loja e região](images/p1_3_loja_regiao.png)

**P1.1 — Categorias:** todas as cinco categorias apresentam taxa entre 24,6% e 25,0%, sem diferenciação significativa. O risco de ruptura é estrutural e uniforme — não está concentrado em um segmento específico. Em contexto real, isso indicaria falha sistêmica na política de reposição.

**P1.2 — Produtos:** os 15 produtos com maior taxa crítica ficam entre 26% e 28%, acima da média geral de 25%. Esses produtos são candidatos prioritários a revisão do ponto de reposição e formação de estoque de segurança dedicado.

**P1.3 — Lojas e regiões:** as 5 lojas e 4 regiões apresentam taxas equivalentes (~25%), sem concentração geográfica. O problema não é operacional de uma unidade — é sistêmico na política de abastecimento da rede.

</details>

<details>
<summary><strong>P2 — Há relação entre alto volume de vendas e maior risco de estoque crítico?</strong></summary>

![Demanda x ruptura](images/p2_demanda.png)

**P2:** este é o achado mais relevante do projeto. Produtos de **baixa demanda concentram 32,4% de dias críticos**, enquanto os de alta demanda têm apenas **1,4%**. O resultado é contraintuitivo: produtos de alto giro recebem mais estoque (média de 387 unidades vs 238) e são menos vulneráveis à ruptura. O risco está nos produtos de menor visibilidade comercial, que recebem reposição insuficiente. A recomendação é priorizar o abastecimento dos produtos de baixa saída, que hoje passam despercebidos pela política de compras.

</details>

<details>
<summary><strong>P3 — Dias com feriado/promoção ativo geram maior pressão sobre o estoque?</strong></summary>

![Feriado/Promoção vs dias normais](images/p3_feriado_promocao.png)

**P3:** dias com o indicador ativo (24,5%) e dias normais (25,0%) apresentam taxas praticamente idênticas. As vendas médias também são equivalentes (136,42 vs 136,51 unidades). O indicador `feriado_ou_promocao_ativo` não discrimina risco neste dataset. **Limitação importante:** a coluna original `holiday_promotion` é um inteiro 0/1 que não distingue feriado de promoção — qualquer separação seria arbitrária. Em dados reais com variação de demanda por evento, esta pergunta revelaria picos de pressão sobre o estoque associados a campanhas promocionais.

</details>

<details>
<summary><strong>P4 — Períodos específicos apresentam maior pressão sobre o estoque?</strong></summary>

![Variação mensal da taxa crítica](images/p4_1_mensal.png)
![Evento vs dias normais](images/p4_2_evento_vs_normal.png)

**P4.1 — Sazonalidade mensal:** há variação sazonal visível: março e setembro registram os menores valores (23,8%) e dezembro o maior (25,6%). Amplitude de 1,8 p.p. modesta, mas o padrão sugere que o final do ano concentra maior pressão — período compatível com datas comemorativas e aumento de demanda típico do varejo. Esses meses devem integrar o calendário de compras com reposição antecipada.

**P4.2 — Evento vs dia normal:** dias com evento (24,5%) e dias normais (25,0%) são estatisticamente equivalentes, pela mesma limitação do indicador binário discutida em P3. A análise mensal (P4.1) é o ângulo mais robusto para identificar sazonalidade neste dataset.

</details>

<details>
<summary><strong>P5 — Há relação entre preço do produto e risco de estoque crítico? (complementar)</strong></summary>

![Preço x ruptura](images/p5_preco_x_ruptura.png)

**P5:** apesar da variação real de preços (R$ 10 a R$ 100, desvio padrão de R$ 26), as taxas críticas são idênticas entre faixas (~25%). O preço não é um fator discriminante neste dataset sintético — o risco de ruptura é determinado pela política de reposição, não pelo valor do produto. Em dados reais de varejo, produtos premium com menor giro poderiam apresentar comportamento diferente.

</details>

---

## Autoavaliação

<details>
<summary><strong>O que foi atingido</strong></summary>

- [x] Pipeline completo Bronze → Silver → Gold implementado em notebooks separados por camada
- [x] Regra de negócio para estoque crítico definida, documentada e justificada (limiar adaptativo p25 de `dias_cobertura`)
- [x] Esquema Estrela com tabela fato (`fato_estoque_diario`) e 3 dimensões (`dim_produto`, `dim_loja`, `dim_data`)
- [x] Catálogo de dados completo com tipos, descrições, domínio de valores e linhagem para todas as camadas (Bronze, Silver, Gold)
- [x] Verificação de qualidade em 5 dimensões: completude, unicidade, consistência, acurácia e outliers
- [x] 5 perguntas de negócio respondidas com análises e visualizações em Python

</details>

<details>
<summary><strong>Limitações e dificuldades</strong></summary>

- **P5 (Preço x Ruptura):** dataset sintético com baixa variação de preços limita o poder analítico.
- **Compatibilidade com Delta Lake:** colunas com espaços nos nomes causaram erro `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` — resolvido renomeando todas para snake_case.
- **Window function com DATE:** `.cast("long")` em coluna `DATE` não é suportado na versão do Spark utilizada — resolvido com `.orderBy("data")`.
- **Coluna feriado_ou_promocao:** a coluna original é inteiro binário (0/1) e não distingue feriado de promoção — criado indicador único `feriado_ou_promocao_ativo`; análises P3 e P4.2 usam o indicador unificado.
- **Databricks Free Edition:** não possui publicação de notebooks com link público.

</details>

<details>
<summary><strong>Trabalhos futuros</strong></summary>

- Preencher descrições das tabelas e colunas diretamente no Unity Catalog
- Criar alertas automáticos para `flag_estoque_critico = True` usando Databricks Workflows
- Explorar modelos de previsão de demanda (Prophet, ARIMA) para antecipar rupturas sazonais
- Construir dashboard interativo no Databricks SQL com atualização diária
- Ampliar a análise com dados de múltiplos períodos para detectar tendências de longo prazo

</details>

---

## Tecnologias

| Ferramenta | Uso |
|---|---|
| Databricks Free Edition | Plataforma de processamento na nuvem (Apache Spark) |
| Delta Lake | Formato de armazenamento com suporte a ACID e versionamento |
| Unity Catalog | Catálogo centralizado de dados e controle de acesso |
| PySpark | Processamento distribuído dos dados |
| Python (Pandas, Matplotlib, Seaborn) | Análise exploratória e visualizações |
| SQL | Consultas analíticas sobre as tabelas Gold |
