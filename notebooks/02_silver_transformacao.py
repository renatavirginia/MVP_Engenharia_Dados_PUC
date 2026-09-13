# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 02: Transformação Silver
# MAGIC **Pipeline:** Retail Store Inventory | Risco de Ruptura de Estoque
# MAGIC
# MAGIC Aqui os dados brutos da Bronze são limpos e organizados. As colunas recebem nomes em
# MAGIC português, os tipos são ajustados, duplicatas são removidas e valores fora do esperado
# MAGIC são tratados antes de salvar na Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, DoubleType, StringType, BooleanType

CATALOG       = "workspace"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
TABLE_IN      = "inventory_raw"
TABLE_OUT     = "inventory_clean"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")

df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.{TABLE_IN}")
print(f"Registros na Bronze: {df_bronze.count():,}")
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Inspeção inicial das colunas originais

# COMMAND ----------

df_bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Renomeação e padronização de colunas
# MAGIC
# MAGIC Mapear nomes originais do dataset para nomes padronizados em português.

# COMMAND ----------

# Mapeamento: nome original → nome padronizado
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

df = df_bronze
for original, novo in rename_map.items():
    if original in df.columns:
        df = df.withColumnRenamed(original, novo)

# Remove colunas de metadados do bronze
df = df.drop("ingestion_date", "source_table")

print("Colunas após renomeação:")
print(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Conversão de tipos

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

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Separação da coluna feriado_ou_promocao
# MAGIC
# MAGIC O dataset sintético pode combinar feriado e promoção em uma única coluna.
# MAGIC Verificamos os valores únicos e criamos colunas booleanas separadas.

# COMMAND ----------

print("Valores únicos em feriado_ou_promocao:")
df.select("feriado_ou_promocao").distinct().show()

# Criar flags separadas com base nos valores encontrados
# Ajustar conforme os valores reais do dataset
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
# MAGIC ## 6. Padronização de strings

# COMMAND ----------

str_cols = ["loja_id", "produto_id", "categoria", "regiao", "condicao_climatica", "sazonalidade"]

for c in str_cols:
    if c in df.columns:
        df = df.withColumn(c, F.trim(F.col(c)))

# Padronização de categoria e região para uppercase
for c in ["categoria", "regiao", "sazonalidade"]:
    if c in df.columns:
        df = df.withColumn(c, F.upper(F.col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Remoção de duplicatas
# MAGIC
# MAGIC A chave natural é: loja + produto + data. Cada combinação deve aparecer uma única vez.

# COMMAND ----------

total_antes = df.count()
df = df.dropDuplicates(["loja_id", "produto_id", "data"])
total_depois = df.count()

print(f"Registros antes : {total_antes:,}")
print(f"Registros depois: {total_depois:,}")
print(f"Duplicatas removidas: {total_antes - total_depois:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tratamento de valores nulos
# MAGIC
# MAGIC Decisões documentadas por coluna:

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum, when, isnan

# Contagem de nulos antes do tratamento
print("=== Nulos por coluna (antes do tratamento) ===")
null_counts = df.select([
    spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df.columns
])
null_counts.show(vertical=True)

# COMMAND ----------

# Tratamento de nulos:
# - nivel_estoque / unidades_vendidas nulos: remover linha (sem essas colunas a análise é inviável)
# - preco_unitario nulo: substituir pela mediana da categoria
# - condicao_climatica nulo: substituir por "NAO_INFORMADO"
# - demais colunas numéricas: substituir por 0

df = df.filter(F.col("nivel_estoque").isNotNull() & F.col("unidades_vendidas").isNotNull())
df = df.filter(F.col("data").isNotNull())

df = df.fillna({"condicao_climatica": "NAO_INFORMADO"})
df = df.fillna({"desconto": 0.0, "preco_concorrente": 0.0, "previsao_demanda": 0.0})

print(f"Registros após tratamento de nulos: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validação de acurácia
# MAGIC
# MAGIC Verificar se existem valores impossíveis no contexto de negócio.

# COMMAND ----------

print("=== Registros com estoque negativo ===")
df.filter(F.col("nivel_estoque") < 0).show(5)

print("=== Registros com vendas negativas ===")
df.filter(F.col("unidades_vendidas") < 0).show(5)

print("=== Registros com preço negativo ou zero ===")
df.filter(F.col("preco_unitario") <= 0).show(5)

# Remover registros com valores impossíveis
df = df.filter(
    (F.col("nivel_estoque") >= 0) &
    (F.col("unidades_vendidas") >= 0) &
    (F.col("preco_unitario") > 0)
)

print(f"\nRegistros após validação de acurácia: {df.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Gravação na camada Silver

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_OUT}")
)

count = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_OUT}").count()
print(f"Tabela {CATALOG}.{SCHEMA_SILVER}.{TABLE_OUT} gravada com {count:,} registros.")
spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_OUT}").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo das Transformações Silver
# MAGIC
# MAGIC | Transformação | Decisão |
# MAGIC |---|---|
# MAGIC | Renomeação de colunas | Nomes padronizados em português |
# MAGIC | Conversão de tipos | `data` → DateType, numéricos → Int/Double |
# MAGIC | Separação feriado/promoção | Criadas colunas booleanas `em_promocao` e `feriado` |
# MAGIC | Padronização de strings | Trim + uppercase em categorias e regiões |
# MAGIC | Duplicatas | Removidas por chave `loja_id + produto_id + data` |
# MAGIC | Nulos em colunas essenciais | Linhas removidas |
# MAGIC | Nulos em colunas secundárias | Substituídos por valor padrão |
# MAGIC | Valores impossíveis | Linhas com estoque/vendas negativos ou preço zero removidas |
# MAGIC
# MAGIC **Próximo passo:** Notebook `03_gold_modelagem` para criação do Esquema Estrela.
