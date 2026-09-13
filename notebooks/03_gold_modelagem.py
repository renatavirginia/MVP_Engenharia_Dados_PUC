# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 03: Modelagem Gold (Esquema Estrela)
# MAGIC **Pipeline:** Retail Store Inventory | Risco de Ruptura de Estoque
# MAGIC
# MAGIC Aqui a camada Gold é construída no formato de Esquema Estrela. É também neste passo
# MAGIC que a regra de negócio entra em ação: calculo os dias de cobertura de estoque e
# MAGIC identifico os registros em situação de risco de ruptura.
# MAGIC
# MAGIC ### Regra de negócio: Flag de Estoque Crítico
# MAGIC O dataset não tem uma coluna explícita de ruptura, então criamos essa métrica a partir dos dados:
# MAGIC
# MAGIC > **`flag_estoque_critico = True`** quando `dias_cobertura < 7`
# MAGIC >
# MAGIC > onde `dias_cobertura = nivel_estoque / demanda_media_7d`
# MAGIC
# MAGIC **Por que 7 dias?** Esse é o critério padrão para varejo de ciclo curto. Abaixo desse limiar
# MAGIC o estoque entra em zona de risco antes do próximo ciclo de reabastecimento.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DateType

CATALOG       = "workspace"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"
TABLE_SILVER  = "inventory_clean"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

df_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.{TABLE_SILVER}")
print(f"Registros na Silver: {df_silver.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dimensão Produto: `gold.dim_produto`

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

print(f"dim_produto: {df_dim_produto.count()} registros")
df_dim_produto.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimensão Loja: `gold.dim_loja`

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

print(f"dim_loja: {df_dim_loja.count()} registros")
df_dim_loja.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dimensão Data: `gold.dim_data`

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
    .select(
        "id_data",
        F.col("data").alias("data_completa"),
        "ano",
        "mes",
        "nome_mes",
        "dia_semana",
        "trimestre",
        "feriado",
    )
    .orderBy("id_data")
)

(
    df_dim_data.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.dim_data")
)

print(f"dim_data: {df_dim_data.count()} registros")
df_dim_data.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cálculo da demanda média e flag de estoque crítico

# COMMAND ----------

# Window: média móvel de 7 dias de vendas por produto e loja
window_7d = (
    Window
    .partitionBy("loja_id", "produto_id")
    .orderBy("data")
    .rowsBetween(-6, 0)  # janela dos últimos 7 dias (incluindo o dia atual)
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
        ).otherwise(999.0)  # sem demanda = sem risco
    )
    .withColumn(
        "flag_estoque_critico",
        F.col("dias_cobertura") < 7
    )
)

print("=== Distribuição da flag de estoque crítico ===")
df_com_metricas.groupBy("flag_estoque_critico").count().show()

print("\n=== Amostra com métricas calculadas ===")
df_com_metricas.select(
    "loja_id", "produto_id", "data",
    "nivel_estoque", "unidades_vendidas",
    "demanda_media_7d", "dias_cobertura", "flag_estoque_critico"
).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tabela Fato: `gold.fato_estoque_diario`

# COMMAND ----------

# Carregar dimensões para fazer JOIN e obter as chaves surrogate
df_dim_produto_ref = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_produto").select("id_produto", "produto_id_orig")
df_dim_loja_ref    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_loja").select("id_loja", "loja_id_orig")
df_dim_data_ref    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_data").select("id_data", "data_completa")

df_fato = (
    df_com_metricas
    # JOIN com dim_produto
    .join(df_dim_produto_ref, df_com_metricas["produto_id"] == df_dim_produto_ref["produto_id_orig"], "left")
    # JOIN com dim_loja
    .join(df_dim_loja_ref, df_com_metricas["loja_id"] == df_dim_loja_ref["loja_id_orig"], "left")
    # JOIN com dim_data
    .join(df_dim_data_ref, df_com_metricas["data"] == df_dim_data_ref["data_completa"], "left")
    .select(
        F.col("id_data"),
        F.col("id_produto"),
        F.col("id_loja"),
        F.col("unidades_vendidas"),
        F.col("unidades_pedidas"),
        F.col("nivel_estoque"),
        F.col("preco_unitario"),
        F.col("desconto"),
        F.col("em_promocao"),
        F.col("feriado"),
        F.col("condicao_climatica"),
        F.col("sazonalidade"),
        F.col("previsao_demanda"),
        F.col("preco_concorrente"),
        F.col("demanda_media_7d"),
        F.col("dias_cobertura"),
        F.col("flag_estoque_critico"),
    )
)

(
    df_fato.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")
)

count = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario").count()
print(f"fato_estoque_diario: {count:,} registros")
spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validação do modelo

# COMMAND ----------

print("=== Contagem por tabela ===")
for tabela in ["dim_produto", "dim_loja", "dim_data", "fato_estoque_diario"]:
    n = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.{tabela}").count()
    print(f"  {tabela}: {n:,}")

print("\n=== Integridade referencial: chaves sem correspondência na fato ===")
fato = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")
print(f"  id_produto nulos: {fato.filter(F.col('id_produto').isNull()).count()}")
print(f"  id_loja nulos   : {fato.filter(F.col('id_loja').isNull()).count()}")
print(f"  id_data nulos   : {fato.filter(F.col('id_data').isNull()).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Modelagem Gold
# MAGIC
# MAGIC | Tabela | Tipo | Registros |
# MAGIC |---|---|---|
# MAGIC | `gold.dim_produto` | Dimensão | N produtos únicos |
# MAGIC | `gold.dim_loja` | Dimensão | N lojas únicas |
# MAGIC | `gold.dim_data` | Dimensão | N datas únicas |
# MAGIC | `gold.fato_estoque_diario` | Fato | ~73k registros |
# MAGIC
# MAGIC ### Regra de ruptura aplicada
# MAGIC - `demanda_media_7d` = média móvel de 7 dias de `unidades_vendidas` por produto+loja
# MAGIC - `dias_cobertura` = `nivel_estoque / demanda_media_7d`
# MAGIC - `flag_estoque_critico` = `dias_cobertura < 7`
# MAGIC
# MAGIC **Próximo passo:** Notebook `04_qualidade_dados` para verificação de qualidade.
