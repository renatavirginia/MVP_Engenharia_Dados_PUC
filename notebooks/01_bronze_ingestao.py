# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 01: Ingestão Bronze
# MAGIC **Pipeline:** Retail Store Inventory | Risco de Ruptura de Estoque
# MAGIC
# MAGIC Primeiro passo deste trabalho. Os dados são lidos da tabela de origem no Unity Catalog
# MAGIC e gravados na Bronze sem nenhuma alteração de conteúdo. A ideia aqui é simples: guardar
# MAGIC o dado exatamente como veio da fonte para ter rastreabilidade total do que foi processado.
# MAGIC
# MAGIC **Fonte:** Retail Store Inventory Forecasting Dataset (Kaggle)
# MAGIC **Autor:** Anirudh Chauhan
# MAGIC **Licença:** CC0 (Public Domain)
# MAGIC **Link:** https://www.kaggle.com/datasets/anirudhchauhan/retail-store-inventory-forecasting-dataset

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração do ambiente

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

SOURCE_TABLE  = "workspace.default.retail_store_inventory"
CATALOG       = "workspace"
SCHEMA_BRONZE = "bronze"
TABLE_BRONZE  = "inventory_raw"

# Renomear colunas para snake_case pois o Delta Lake nao aceita espacos nos nomes
RENAME_MAP = {
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

print(f"Origem : {SOURCE_TABLE}")
print(f"Destino: {CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criação do schema Bronze

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")
print(f"Schema {CATALOG}.{SCHEMA_BRONZE} pronto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura da tabela de origem

# COMMAND ----------

df_raw = spark.table(SOURCE_TABLE)

for original, novo in RENAME_MAP.items():
    if original in df_raw.columns:
        df_raw = df_raw.withColumnRenamed(original, novo)

print(f"Linhas lidas: {df_raw.count():,}")
print(f"Colunas     : {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Adição de metadados de controle

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("ingestion_date", current_timestamp())
    .withColumn("source_table", lit(SOURCE_TABLE))
)

df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gravação na tabela Delta Bronze

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}")
)

count = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}").count()
print(f"Tabela {CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE} gravada com {count:,} registros.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação da ingestão

# COMMAND ----------

df_check = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_BRONZE}")

print("=== Primeiras linhas ===")
df_check.show(5, truncate=False)

print("\n=== Contagem de nulos por coluna ===")
from pyspark.sql.functions import col, sum as spark_sum, isnan, when

null_counts = df_check.select([
    spark_sum(when(col(c).isNull() | isnan(c), 1).otherwise(0)).alias(c)
    if df_check.schema[c].dataType.typeName() in ("double", "float", "integer", "long")
    else spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_check.columns
    if c not in ("ingestion_date", "source_table")
])
null_counts.show(vertical=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Ingestão Bronze
# MAGIC
# MAGIC | Item | Valor |
# MAGIC |---|---|
# MAGIC | Tabela destino | `workspace.bronze.inventory_raw` |
# MAGIC | Formato | Delta Lake |
# MAGIC | Dados alterados | Nenhum (dado bruto preservado) |
# MAGIC | Metadados adicionados | `ingestion_date`, `source_table` |
# MAGIC
# MAGIC **Próximo passo:** Notebook `02_silver_transformacao` para limpeza e padronização dos dados.
