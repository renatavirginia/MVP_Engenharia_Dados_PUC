# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Completo: Risco de Ruptura de Estoque no Varejo
# MAGIC
# MAGIC **Nome:** Renata Virginia<br>
# MAGIC **Matrícula:** 4052025002400
# MAGIC
# MAGIC Este notebook executa todas as etapas do pipeline de uma vez, do dado bruto às análises finais.
# MAGIC
# MAGIC | Fase | Descrição |
# MAGIC |---|---|
# MAGIC | 1 | Ingestão Bronze: leitura da tabela de origem e gravação sem transformações |
# MAGIC | 2 | Transformação Silver: limpeza, padronização e remoção de duplicatas |
# MAGIC | 3 | Modelagem Gold: Esquema Estrela e regra de estoque crítico |
# MAGIC | 4 | Qualidade de Dados: completude, unicidade, consistência, acurácia e outliers |
# MAGIC | 5 | Análise de Negócio: resposta às cinco perguntas com visualizações |
# MAGIC
# MAGIC **Dataset:** Retail Store Inventory Forecasting Dataset (Kaggle | Anirudh Chauhan | CC0)
# MAGIC
# MAGIC **Regra de negócio:**
# MAGIC > `flag_estoque_critico = True` quando `dias_cobertura < 7`
# MAGIC > onde `dias_cobertura = nivel_estoque / demanda_media_7d`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuração Geral

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DateType, IntegerType, DoubleType, StringType, BooleanType
from pyspark.sql.functions import current_timestamp, lit, col, sum as spark_sum, when, isnan
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

# Constantes globais
SOURCE_TABLE  = "workspace.default.retail_store_inventory"
CATALOG       = "workspace"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"
TABLE_BRONZE  = "inventory_raw"
TABLE_SILVER  = "inventory_clean"

# Renomear colunas para snake_case pois o Delta Lake nao aceita espacos nos nomes
RENAME_MAP_BRONZE = {
    "Date"              : "date",
    "Store ID"          : "store_id",
    "Product ID"        : "product_id",
    "Category"          : "category",
    "Region"            : "region",
    "Inventory Level"   : "inventory_level",
    "Units Sold"        : "units_sold",
    "Units Ordered"     : "units_ordered",
    "Demand Forecast"   : "demand_forecast",
    "Price"             : "price",
    "Discount"          : "discount",
    "Weather Condition" : "weather_condition",
    "Holiday/Promotion" : "holiday_promotion",
    "Competitor Pricing": "competitor_pricing",
    "Seasonality"       : "seasonality",
}

# Estilo dos gráficos
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 5)
plt.rcParams["axes.titlesize"] = 13

print("Configuração carregada.")
print(f"  Origem  : {SOURCE_TABLE}")
print(f"  Catálogo: {CATALOG}")
print(f"  Schemas : {SCHEMA_BRONZE} | {SCHEMA_SILVER} | {SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Fase 1: Ingestão Bronze
# MAGIC
# MAGIC Os dados são lidos da tabela de origem no Unity Catalog e gravados na Bronze sem nenhuma
# MAGIC alteração de conteúdo. As colunas são renomeadas para snake_case para compatibilidade com
# MAGIC o Delta Lake. Metadados adicionados: `ingestion_date` e `source_table`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.1 Criação do schema Bronze

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
print(f"Schema {CATALOG}.{SCHEMA_BRONZE} pronto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.2 Leitura da tabela de origem

# COMMAND ----------

df_raw = spark.table(SOURCE_TABLE)

for original, novo in RENAME_MAP_BRONZE.items():
    if original in df_raw.columns:
        df_raw = df_raw.withColumnRenamed(original, novo)

print(f"Linhas lidas: {df_raw.count():,}")
print(f"Colunas     : {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.3 Adição de metadados e gravação Delta

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("source_table", lit(SOURCE_TABLE))
)

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}")
)

count_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}").count()
print(f"[OK] {CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE} gravada com {count_bronze:,} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1.4 Validação da ingestão

# COMMAND ----------

df_check_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}")
df_check_bronze.show(3, truncate=False)

print("=== Nulos por coluna (Bronze) ===")
null_counts_bronze = df_check_bronze.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_check_bronze.columns
    if c not in ("ingestion_date", "source_table")
])
null_counts_bronze.show(vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Fase 2: Transformação Silver
# MAGIC
# MAGIC Aqui os dados brutos da Bronze são limpos e organizados. As colunas recebem nomes em
# MAGIC português, os tipos são ajustados, duplicatas são removidas e valores fora do esperado
# MAGIC são tratados antes de salvar na Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.1 Leitura da Bronze e criação do schema Silver

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")

df = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}")
print(f"Registros na Bronze: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.2 Renomeação de colunas para português

# COMMAND ----------

rename_map = {
    "store_id"          : "loja_id",
    "product_id"        : "produto_id",
    "category"          : "categoria",
    "region"            : "regiao",
    "date"              : "data",
    "inventory_level"   : "nivel_estoque",
    "units_sold"        : "unidades_vendidas",
    "units_ordered"     : "unidades_pedidas",
    "demand_forecast"   : "previsao_demanda",
    "price"             : "preco_unitario",
    "discount"          : "desconto",
    "weather_condition" : "condicao_climatica",
    "holiday_promotion" : "feriado_ou_promocao",
    "competitor_pricing": "preco_concorrente",
    "seasonality"       : "sazonalidade",
}

for original, novo in rename_map.items():
    if original in df.columns:
        df = df.withColumnRenamed(original, novo)

df = df.drop("ingestion_date", "source_table")
print("Colunas após renomeação:", df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.3 Conversão de tipos

# COMMAND ----------

df = (
    df
    .withColumn("data",               F.to_date(F.col("data")))
    .withColumn("nivel_estoque",      F.col("nivel_estoque").cast(IntegerType()))
    .withColumn("unidades_vendidas",  F.col("unidades_vendidas").cast(IntegerType()))
    .withColumn("unidades_pedidas",   F.col("unidades_pedidas").cast(IntegerType()))
    .withColumn("previsao_demanda",   F.col("previsao_demanda").cast(DoubleType()))
    .withColumn("preco_unitario",     F.col("preco_unitario").cast(DoubleType()))
    .withColumn("desconto",           F.col("desconto").cast(DoubleType()))
    .withColumn("preco_concorrente",  F.col("preco_concorrente").cast(DoubleType()))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.4 Separação da coluna feriado/promoção

# COMMAND ----------

print("Valores únicos em feriado_ou_promocao:")
df.select("feriado_ou_promocao").distinct().show()

df = df.withColumn(
    "em_promocao",
    F.when(F.lower(F.col("feriado_ou_promocao")).contains("promotion"), True)
     .when(F.lower(F.col("feriado_ou_promocao")).contains("promo"), True)
     .when(F.col("feriado_ou_promocao") == "1", True)
     .otherwise(False)
).withColumn(
    "feriado",
    F.when(F.lower(F.col("feriado_ou_promocao")).contains("holiday"), True)
     .when(F.lower(F.col("feriado_ou_promocao")).contains("feriado"), True)
     .otherwise(False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.5 Padronização de strings

# COMMAND ----------

for c in ["loja_id", "produto_id", "categoria", "regiao", "condicao_climatica", "sazonalidade"]:
    if c in df.columns:
        df = df.withColumn(c, F.trim(F.col(c)))

for c in ["categoria", "regiao", "sazonalidade"]:
    if c in df.columns:
        df = df.withColumn(c, F.upper(F.col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.6 Remoção de duplicatas

# COMMAND ----------

total_antes = df.count()
df = df.dropDuplicates(["loja_id", "produto_id", "data"])
total_depois = df.count()

print(f"Registros antes        : {total_antes:,}")
print(f"Registros depois       : {total_depois:,}")
print(f"Duplicatas removidas   : {total_antes - total_depois:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.7 Tratamento de nulos e valores impossíveis

# COMMAND ----------

print("=== Nulos por coluna (antes do tratamento) ===")
df.select([spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]).show(vertical=True)

# Essenciais: remover linha se nulo
df = df.filter(F.col("nivel_estoque").isNotNull() & F.col("unidades_vendidas").isNotNull())
df = df.filter(F.col("data").isNotNull())

# Secundários: substituir por padrão
df = df.fillna({"condicao_climatica": "NAO_INFORMADO"})
df = df.fillna({"desconto": 0.0, "preco_concorrente": 0.0, "previsao_demanda": 0.0})

# Valores impossíveis
df = df.filter(
    (F.col("nivel_estoque") >= 0) &
    (F.col("unidades_vendidas") >= 0) &
    (F.col("preco_unitario") > 0)
)

print(f"Registros após tratamento: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2.8 Gravação na camada Silver

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_SILVER}")
)

count_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_SILVER}").count()
print(f"[OK] {CATALOG}.{SCHEMA_SILVER}.{TABLE_SILVER} gravada com {count_silver:,} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Fase 3: Modelagem Gold (Esquema Estrela)
# MAGIC
# MAGIC Aqui a camada Gold é construída no formato de Esquema Estrela. É também neste passo
# MAGIC que a regra de negócio entra em ação: calculo os dias de cobertura de estoque e
# MAGIC identifico os registros em situação de risco de ruptura.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1 Leitura da Silver e criação do schema Gold

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

df_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_SILVER}")
print(f"Registros na Silver: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2 Dimensão Produto: `gold.dim_produto`

# COMMAND ----------

df_dim_produto = (
    df_silver
    .select("produto_id", "categoria")
    .distinct()
    .orderBy("produto_id")
    .withColumn("id_produto", F.monotonically_increasing_id().cast(IntegerType()))
    .select(
        F.col("id_produto"),
        F.col("produto_id").alias("produto_id_orig"),
        F.col("produto_id").alias("nome_produto"),
        F.col("categoria"),
    )
)

(
    df_dim_produto.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_produto")
)

print(f"[OK] dim_produto: {df_dim_produto.count()} registros")
df_dim_produto.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3 Dimensão Loja: `gold.dim_loja`

# COMMAND ----------

df_dim_loja = (
    df_silver
    .select("loja_id", "regiao")
    .distinct()
    .orderBy("loja_id")
    .withColumn("id_loja", F.monotonically_increasing_id().cast(IntegerType()))
    .select(
        F.col("id_loja"),
        F.col("loja_id").alias("loja_id_orig"),
        F.col("loja_id").alias("nome_loja"),
        F.col("regiao"),
    )
)

(
    df_dim_loja.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_loja")
)

print(f"[OK] dim_loja: {df_dim_loja.count()} registros")
df_dim_loja.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.4 Dimensão Data: `gold.dim_data`

# COMMAND ----------

df_dim_data = (
    df_silver
    .select("data", "feriado")
    .distinct()
    .withColumn("id_data",    F.date_format(F.col("data"), "yyyyMMdd").cast(IntegerType()))
    .withColumn("ano",        F.year(F.col("data")))
    .withColumn("mes",        F.month(F.col("data")))
    .withColumn("nome_mes",   F.date_format(F.col("data"), "MMMM"))
    .withColumn("dia_semana", F.dayofweek(F.col("data")))
    .withColumn("trimestre",  F.quarter(F.col("data")))
    .select("id_data", F.col("data").alias("data_completa"), "ano", "mes", "nome_mes", "dia_semana", "trimestre", "feriado")
    .orderBy("id_data")
)

(
    df_dim_data.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_data")
)

print(f"[OK] dim_data: {df_dim_data.count()} registros")
df_dim_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.5 Cálculo da demanda média e flag de estoque crítico

# COMMAND ----------

window_7d = (
    Window
    .partitionBy("loja_id", "produto_id")
    .orderBy("data")
    .rowsBetween(-6, 0)
)

df_com_metricas = (
    df_silver
    .withColumn(
        "demanda_media_7d",
        F.round(F.avg("unidades_vendidas").over(window_7d), 2)
    )
    .withColumn(
        "dias_cobertura",
        F.when(
            F.col("demanda_media_7d") > 0,
            F.round(F.col("nivel_estoque") / F.col("demanda_media_7d"), 2)
        ).otherwise(999.0)
    )
    .withColumn(
        "flag_estoque_critico",
        F.col("dias_cobertura") < 7
    )
)

print("=== Distribuição da flag de estoque crítico ===")
df_com_metricas.groupBy("flag_estoque_critico").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.6 Tabela Fato: `gold.fato_estoque_diario`

# COMMAND ----------

df_dim_produto_ref = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_produto").select("id_produto", "produto_id_orig")
df_dim_loja_ref    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_loja").select("id_loja", "loja_id_orig")
df_dim_data_ref    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_data").select("id_data", "data_completa")

df_fato = (
    df_com_metricas
    .join(df_dim_produto_ref, df_com_metricas["produto_id"] == df_dim_produto_ref["produto_id_orig"], "left")
    .join(df_dim_loja_ref,    df_com_metricas["loja_id"]    == df_dim_loja_ref["loja_id_orig"],       "left")
    .join(df_dim_data_ref,    df_com_metricas["data"]       == df_dim_data_ref["data_completa"],      "left")
    .select(
        "id_data", "id_produto", "id_loja",
        "unidades_vendidas", "unidades_pedidas", "nivel_estoque",
        "preco_unitario", "desconto", "em_promocao", "feriado",
        "condicao_climatica", "sazonalidade", "previsao_demanda", "preco_concorrente",
        "demanda_media_7d", "dias_cobertura", "flag_estoque_critico",
    )
)

(
    df_fato.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")
)

count_fato = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario").count()
print(f"[OK] fato_estoque_diario: {count_fato:,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.7 Validação do modelo

# COMMAND ----------

print("=== Contagem por tabela Gold ===")
for tabela in ["dim_produto", "dim_loja", "dim_data", "fato_estoque_diario"]:
    n = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.{tabela}").count()
    print(f"  {tabela:30s}: {n:,}")

fato_val = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")
print("\n=== Integridade referencial (chaves nulas na fato) ===")
print(f"  id_produto nulos: {fato_val.filter(F.col('id_produto').isNull()).count()}")
print(f"  id_loja nulos   : {fato_val.filter(F.col('id_loja').isNull()).count()}")
print(f"  id_data nulos   : {fato_val.filter(F.col('id_data').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Fase 4: Qualidade de Dados
# MAGIC
# MAGIC Verifico a qualidade dos dados nas três camadas do pipeline e documento o que foi
# MAGIC encontrado em cada ponto e as decisões tomadas ao longo do processo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 Carregamento das tabelas para verificação

# COMMAND ----------

df_bronze_q = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}")
df_silver_q = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_SILVER}")
df_fato_q   = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")

print(f"Bronze: {df_bronze_q.count():,} registros")
print(f"Silver: {df_silver_q.count():,} registros")
print(f"Fato  : {df_fato_q.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 Completude: Valores Nulos

# COMMAND ----------

def relatorio_nulos(df, nome_tabela):
    total = df.count()
    print(f"\n=== {nome_tabela} | Total: {total:,} registros ===")
    for c in df.columns:
        n_nulos = df.filter(F.col(c).isNull()).count()
        pct = round(n_nulos * 100 / total, 2) if total > 0 else 0
        status = "OK     " if n_nulos == 0 else "ATENCAO"
        print(f"  [{status}] {c:35s} → {n_nulos:6,} nulos ({pct}%)")

relatorio_nulos(df_bronze_q, "BRONZE: inventory_raw")
relatorio_nulos(df_silver_q, "SILVER: inventory_clean")
relatorio_nulos(df_fato_q,   "GOLD: fato_estoque_diario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 Unicidade: Duplicatas

# COMMAND ----------

total_silver  = df_silver_q.count()
distinct_keys = df_silver_q.select("loja_id", "produto_id", "data").distinct().count()
duplicatas    = total_silver - distinct_keys

print(f"Total de registros   : {total_silver:,}")
print(f"Chaves únicas        : {distinct_keys:,}")
print(f"Duplicatas detectadas: {duplicatas:,}")

if duplicatas == 0:
    print("[OK] Nenhuma duplicata encontrada.")
else:
    print("[ATENCAO] Existem duplicatas.")
    df_silver_q.groupBy("loja_id", "produto_id", "data") \
               .count().filter(F.col("count") > 1) \
               .orderBy(F.col("count").desc()).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 Consistência: Domínios e Formatos

# COMMAND ----------

print("=== Faixa de datas ===")
df_silver_q.select(F.min("data").alias("data_minima"), F.max("data").alias("data_maxima")).show()

print("=== Categorias únicas ===")
df_silver_q.select("categoria").distinct().orderBy("categoria").show(truncate=False)

print("=== Regiões únicas ===")
df_silver_q.select("regiao").distinct().orderBy("regiao").show(truncate=False)

print("=== Condições climáticas únicas ===")
df_silver_q.select("condicao_climatica").distinct().orderBy("condicao_climatica").show(truncate=False)

print("=== Sazonalidade únicas ===")
df_silver_q.select("sazonalidade").distinct().orderBy("sazonalidade").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 Acurácia: Valores Impossíveis

# COMMAND ----------

print(f"Estoque negativo      : {df_silver_q.filter(F.col('nivel_estoque') < 0).count()}")
print(f"Vendas negativas      : {df_silver_q.filter(F.col('unidades_vendidas') < 0).count()}")
print(f"Preço zero ou negativo: {df_silver_q.filter(F.col('preco_unitario') <= 0).count()}")
print(f"Cobertura negativa    : {df_fato_q.filter(F.col('dias_cobertura') < 0).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.6 Outliers: Estatísticas Descritivas

# COMMAND ----------

print("=== Estatísticas descritivas (Silver) ===")
df_silver_q.select("nivel_estoque", "unidades_vendidas", "preco_unitario", "desconto", "previsao_demanda").describe().show()

print("=== Estatísticas de cobertura (Gold) ===")
df_fato_q.select("dias_cobertura", "demanda_media_7d", "nivel_estoque").describe().show()

print("=== Percentis ===")
df_silver_q.select(
    F.percentile_approx("nivel_estoque",    [0.01, 0.25, 0.5, 0.75, 0.99]).alias("percentis_estoque"),
    F.percentile_approx("unidades_vendidas",[0.01, 0.25, 0.5, 0.75, 0.99]).alias("percentis_vendas")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.7 Distribuição da Flag de Estoque Crítico

# COMMAND ----------

print("=== Distribuição geral da flag ===")
df_fato_q.groupBy("flag_estoque_critico") \
    .agg(
        F.count("*").alias("registros"),
        F.round(F.count("*") * 100 / df_fato_q.count(), 2).alias("percentual")
    ).show()

print("=== Faixas de cobertura ===")
df_fato_q.select(
    F.when(F.col("dias_cobertura") < 7,  "< 7 dias (critico)")
     .when(F.col("dias_cobertura") < 14, "7-14 dias (atencao)")
     .when(F.col("dias_cobertura") < 30, "14-30 dias (ok)")
     .otherwise("> 30 dias (folgado)").alias("faixa_cobertura")
).groupBy("faixa_cobertura").count().orderBy("faixa_cobertura").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # Fase 5: Análise de Negócio
# MAGIC
# MAGIC Aqui respondo às cinco perguntas de negócio definidas no início do projeto, usando
# MAGIC consultas sobre a camada Gold e visualizações em Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5.1 Preparação do dataset analítico

# COMMAND ----------

df_fato_a    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")
df_produto_a = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_produto")
df_loja_a    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_loja")
df_data_a    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_data")

df_analitico = (
    df_fato_a
    .join(df_produto_a,                "id_produto", "left")
    .join(df_loja_a,                   "id_loja",    "left")
    .join(df_data_a.drop("feriado"),   "id_data",    "left")
)

print(f"Dataset analítico: {df_analitico.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## P1: Quais produtos, categorias e lojas têm maior frequência de estoque crítico?

# COMMAND ----------

# MAGIC %md
# MAGIC ### P1.1: Por categoria

# COMMAND ----------

df_p1_cat = (
    df_analitico
    .groupBy("categoria")
    .agg(
        F.count("*").alias("total_dias"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("dias_criticos")
    )
    .withColumn("pct_critico", F.round(F.col("dias_criticos") * 100 / F.col("total_dias"), 1))
    .orderBy(F.col("pct_critico").desc())
).toPandas()

fig, ax = plt.subplots()
bars = ax.barh(df_p1_cat["categoria"], df_p1_cat["pct_critico"], color=sns.color_palette("Reds_r", len(df_p1_cat)))
ax.set_xlabel("% de dias com estoque crítico")
ax.set_title("P1: Taxa de estoque crítico por categoria")
ax.xaxis.set_major_formatter(mtick.PercentFormatter())
for bar, val in zip(bars, df_p1_cat["pct_critico"]):
    ax.text(val + 0.3, bar.get_y() + bar.get_height() / 2, f"{val}%", va="center", fontsize=9)
plt.tight_layout()
plt.show()

print(df_p1_cat.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### P1.2: Top 15 produtos

# COMMAND ----------

df_p1_prod = (
    df_analitico
    .groupBy("produto_id_orig", "categoria")
    .agg(
        F.count("*").alias("total_dias"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("dias_criticos")
    )
    .withColumn("pct_critico", F.round(F.col("dias_criticos") * 100 / F.col("total_dias"), 1))
    .orderBy(F.col("pct_critico").desc())
    .limit(15)
).toPandas()

fig, ax = plt.subplots(figsize=(12, 6))
ax.barh(
    df_p1_prod["produto_id_orig"] + " (" + df_p1_prod["categoria"] + ")",
    df_p1_prod["pct_critico"],
    color=sns.color_palette("OrRd_r", len(df_p1_prod))
)
ax.set_xlabel("% de dias com estoque crítico")
ax.set_title("Top 15 produtos com maior taxa de estoque crítico")
ax.xaxis.set_major_formatter(mtick.PercentFormatter())
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### P1.3: Por loja e região

# COMMAND ----------

df_p1_loja = (
    df_analitico
    .groupBy("loja_id_orig", "regiao")
    .agg(
        F.count("*").alias("total_dias"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("dias_criticos")
    )
    .withColumn("pct_critico", F.round(F.col("dias_criticos") * 100 / F.col("total_dias"), 1))
    .orderBy(F.col("pct_critico").desc())
).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))
axes[0].barh(df_p1_loja["loja_id_orig"], df_p1_loja["pct_critico"], color="steelblue")
axes[0].set_title("Taxa de estoque crítico por loja")
axes[0].set_xlabel("% dias críticos")
axes[0].xaxis.set_major_formatter(mtick.PercentFormatter())

df_p1_reg = df_p1_loja.groupby("regiao")["pct_critico"].mean().reset_index().sort_values("pct_critico", ascending=False)
axes[1].bar(df_p1_reg["regiao"], df_p1_reg["pct_critico"], color=sns.color_palette("muted"))
axes[1].set_title("Taxa média de estoque crítico por região")
axes[1].set_ylabel("% dias críticos (média)")
axes[1].yaxis.set_major_formatter(mtick.PercentFormatter())

plt.suptitle("P1: Estoque crítico por loja e região", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação P1:**
# MAGIC Identificar as categorias, produtos e lojas com maior % de dias em situação de estoque crítico
# MAGIC permite direcionar ações de abastecimento com maior precisão. Categorias com taxa acima da
# MAGIC média geral requerem revisão do ponto de reposição ou da frequência de pedidos.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## P2: Há relação entre alto volume de vendas e maior risco de estoque crítico?

# COMMAND ----------

p75_vendas = df_analitico.approxQuantile("unidades_vendidas", [0.75], 0.01)[0]
print(f"Percentil 75 de unidades vendidas: {p75_vendas}")

df_p2 = (
    df_analitico
    .withColumn(
        "faixa_demanda",
        F.when(F.col("unidades_vendidas") >= p75_vendas, "Alta demanda (≥ P75)")
         .otherwise("Baixa demanda (< P75)")
    )
    .groupBy("faixa_demanda")
    .agg(
        F.count("*").alias("total_registros"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("dias_criticos"),
        F.round(F.avg("nivel_estoque"), 1).alias("estoque_medio"),
        F.round(F.avg("unidades_vendidas"), 1).alias("vendas_medias")
    )
    .withColumn("taxa_ruptura", F.round(F.col("dias_criticos") * 100 / F.col("total_registros"), 1))
).toPandas()

print(df_p2.to_string(index=False))

fig, axes = plt.subplots(1, 2, figsize=(12, 5))
cores = ["#e74c3c" if "Alta" in x else "#3498db" for x in df_p2["faixa_demanda"]]

axes[0].bar(df_p2["faixa_demanda"], df_p2["taxa_ruptura"], color=cores)
axes[0].set_title("Taxa de estoque crítico por faixa de demanda")
axes[0].set_ylabel("% dias críticos")
axes[0].yaxis.set_major_formatter(mtick.PercentFormatter())
for i, v in enumerate(df_p2["taxa_ruptura"]):
    axes[0].text(i, v + 0.5, f"{v}%", ha="center", fontweight="bold")

axes[1].bar(df_p2["faixa_demanda"], df_p2["estoque_medio"], color=cores)
axes[1].set_title("Nível médio de estoque por faixa de demanda")
axes[1].set_ylabel("Estoque médio (unidades)")
for i, v in enumerate(df_p2["estoque_medio"]):
    axes[1].text(i, v + 1, f"{v}", ha="center", fontweight="bold")

plt.suptitle("P2: Relação entre demanda e risco de estoque crítico", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação P2:**
# MAGIC Comparar a taxa de estoque crítico entre produtos de alta e baixa demanda revela se o
# MAGIC problema é estrutural (gestão inadequada independente do volume) ou se itens de maior
# MAGIC giro têm sistematicamente mais risco de ruptura.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## P3: Promoções geram maior pressão sobre o estoque?

# COMMAND ----------

df_p3 = (
    df_analitico
    .groupBy("em_promocao")
    .agg(
        F.count("*").alias("total_registros"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("dias_criticos"),
        F.round(F.avg("unidades_vendidas"), 2).alias("vendas_medias"),
        F.round(F.avg("nivel_estoque"), 2).alias("estoque_medio")
    )
    .withColumn("taxa_critico", F.round(F.col("dias_criticos") * 100 / F.col("total_registros"), 1))
    .withColumn("rotulo", F.when(F.col("em_promocao"), "Com promoção").otherwise("Sem promoção"))
).toPandas()

print(df_p3[["rotulo", "total_registros", "dias_criticos", "taxa_critico", "vendas_medias", "estoque_medio"]].to_string(index=False))

fig, axes = plt.subplots(1, 3, figsize=(15, 5))
cores = ["#e67e22", "#2ecc71"]

for ax, col_name, titulo in zip(axes,
    ["taxa_critico", "vendas_medias", "estoque_medio"],
    ["Taxa de estoque crítico (%)", "Vendas médias (un.)", "Estoque médio (un.)"]):
    bars = ax.bar(df_p3["rotulo"], df_p3[col_name], color=cores)
    ax.set_title(titulo)
    for bar, val in zip(bars, df_p3[col_name]):
        ax.text(bar.get_x() + bar.get_width() / 2, val + 0.3, f"{val}", ha="center", fontweight="bold")

plt.suptitle("P3: Impacto das promoções no estoque", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação P3:**
# MAGIC Se períodos promocionais apresentam taxa de estoque crítico significativamente maior,
# MAGIC isso indica que o planejamento de abastecimento não acompanha o aumento de demanda
# MAGIC gerado pelas promoções. A recomendação é reforçar o estoque de segurança antes de
# MAGIC campanhas promocionais.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## P4: Feriados e períodos específicos apresentam maior pressão sobre o estoque?

# COMMAND ----------

# MAGIC %md
# MAGIC ### P4.1: Variação mensal

# COMMAND ----------

df_p4_mensal = (
    df_analitico
    .groupBy("mes", "nome_mes")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("criticos")
    )
    .withColumn("taxa", F.round(F.col("criticos") * 100 / F.col("total"), 1))
    .orderBy("mes")
).toPandas()

fig, ax = plt.subplots(figsize=(13, 5))
ax.plot(df_p4_mensal["nome_mes"], df_p4_mensal["taxa"], marker="o", color="#e74c3c", linewidth=2)
ax.fill_between(df_p4_mensal["nome_mes"], df_p4_mensal["taxa"], alpha=0.15, color="#e74c3c")
ax.set_title("P4: Taxa de estoque crítico por mês")
ax.set_ylabel("% dias críticos")
ax.yaxis.set_major_formatter(mtick.PercentFormatter())
plt.xticks(rotation=30, ha="right")
for i, row in df_p4_mensal.iterrows():
    ax.annotate(f"{row['taxa']}%", (row['nome_mes'], row['taxa']),
                textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### P4.2: Feriados vs dias normais

# COMMAND ----------

df_p4_feriado = (
    df_analitico
    .groupBy("feriado")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("criticos"),
        F.round(F.avg("unidades_vendidas"), 2).alias("vendas_medias")
    )
    .withColumn("taxa", F.round(F.col("criticos") * 100 / F.col("total"), 1))
    .withColumn("rotulo", F.when(F.col("feriado"), "Feriado").otherwise("Dia normal"))
).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(11, 5))
cores = ["#9b59b6", "#95a5a6"]

axes[0].bar(df_p4_feriado["rotulo"], df_p4_feriado["taxa"], color=cores)
axes[0].set_title("Taxa de estoque crítico")
axes[0].set_ylabel("% dias críticos")
axes[0].yaxis.set_major_formatter(mtick.PercentFormatter())
for i, v in enumerate(df_p4_feriado["taxa"]):
    axes[0].text(i, v + 0.3, f"{v}%", ha="center", fontweight="bold")

axes[1].bar(df_p4_feriado["rotulo"], df_p4_feriado["vendas_medias"], color=cores)
axes[1].set_title("Vendas médias por dia")
axes[1].set_ylabel("Unidades vendidas (média)")
for i, v in enumerate(df_p4_feriado["vendas_medias"]):
    axes[1].text(i, v + 0.1, f"{v}", ha="center", fontweight="bold")

plt.suptitle("P4: Feriados vs dias normais", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

print(df_p4_feriado[["rotulo", "total", "criticos", "taxa", "vendas_medias"]].to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação P4:**
# MAGIC Meses ou feriados com picos de demanda não acompanhados por reforço no estoque geram
# MAGIC pressão crítica. Esses padrões sazonais são previsíveis e devem ser incorporados no
# MAGIC planejamento de reposição com antecedência.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## P5 (Complementar): Há relação entre preço e risco de estoque crítico?

# COMMAND ----------

stats_preco = df_analitico.select(
    F.min("preco_unitario").alias("min"),
    F.max("preco_unitario").alias("max"),
    F.stddev("preco_unitario").alias("desvio_padrao"),
    F.countDistinct("preco_unitario").alias("valores_distintos")
).toPandas()

print("=== Variação de preços no dataset ===")
print(stats_preco.to_string(index=False))

desvio = float(stats_preco["desvio_padrao"].iloc[0])
if desvio < 1.0:
    print("AVISO: Baixa variação de preços detectada. Análise de P5 tem relevância limitada.")

# COMMAND ----------

p25, p75 = df_analitico.approxQuantile("preco_unitario", [0.25, 0.75], 0.01)

df_p5 = (
    df_analitico
    .withColumn(
        "faixa_preco",
        F.when(F.col("preco_unitario") <= p25, "Baixo preço (≤ P25)")
         .when(F.col("preco_unitario") <= p75, "Médio preço (P25-P75)")
         .otherwise("Alto preço (> P75)")
    )
    .groupBy("faixa_preco")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("flag_estoque_critico").cast("int")).alias("criticos"),
        F.round(F.avg("unidades_vendidas"), 2).alias("vendas_medias")
    )
    .withColumn("taxa", F.round(F.col("criticos") * 100 / F.col("total"), 1))
    .orderBy("faixa_preco")
).toPandas()

print(df_p5.to_string(index=False))

fig, ax = plt.subplots(figsize=(9, 5))
ax.bar(df_p5["faixa_preco"], df_p5["taxa"], color=sns.color_palette("Blues_d", 3))
ax.set_title("P5: Taxa de estoque crítico por faixa de preço")
ax.set_ylabel("% dias críticos")
ax.yaxis.set_major_formatter(mtick.PercentFormatter())
for i, v in enumerate(df_p5["taxa"]):
    ax.text(i, v + 0.3, f"{v}%", ha="center", fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação P5:**
# MAGIC Se produtos de maior preço apresentam menor taxa de estoque crítico, isso pode indicar
# MAGIC que itens mais valiosos recebem maior atenção na gestão de estoque. O inverso pode
# MAGIC sinalizar oportunidade de melhoria no controle de itens de alto valor.
# MAGIC
# MAGIC **Nota:** Caso o dataset sintético apresente baixa variação de preços, esta análise
# MAGIC tem relevância limitada e deve ser registrada como tal na autoavaliação.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Discussão Geral dos Resultados
# MAGIC
# MAGIC As análises realizadas permitem identificar:
# MAGIC
# MAGIC 1. **Produtos e lojas de maior risco:** As categorias e lojas com maior % de dias em
# MAGIC    estoque crítico são alvos prioritários para revisão da política de reposição.
# MAGIC
# MAGIC 2. **Demanda como fator de risco:** Produtos com maior volume de vendas apresentam
# MAGIC    naturalmente maior pressão sobre o estoque, especialmente quando o reabastecimento
# MAGIC    não é proporcional ao giro.
# MAGIC
# MAGIC 3. **Promoções como gatilho:** Períodos promocionais concentram picos de demanda que,
# MAGIC    sem planejamento antecipado de estoque, aumentam significativamente o risco de ruptura.
# MAGIC
# MAGIC 4. **Sazonalidade previsível:** A variação mensal da taxa de estoque crítico revela
# MAGIC    padrões sazonais que podem ser incorporados em modelos de previsão de demanda.
# MAGIC
# MAGIC 5. **Preço:** Fator complementar com relevância dependente da variação existente nos dados.
# MAGIC
# MAGIC **Recomendação geral:** Implementar políticas de estoque de segurança diferenciadas por
# MAGIC categoria, loja e período, com reforço preventivo antes de datas promocionais e feriados.
