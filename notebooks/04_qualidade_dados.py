# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 04: Qualidade de Dados
# MAGIC **Pipeline:** Retail Store Inventory | Risco de Ruptura de Estoque
# MAGIC
# MAGIC Neste notebook verifico a qualidade dos dados nas três camadas do pipeline e documento
# MAGIC o que foi encontrado em cada ponto e as decisões que tomei ao longo do processo.
# MAGIC
# MAGIC Avaliamos cinco dimensões de qualidade:
# MAGIC - **Completude:** há valores nulos ou vazios?
# MAGIC - **Consistência:** os valores estão dentro do domínio esperado?
# MAGIC - **Unicidade:** existem duplicatas onde não deveria?
# MAGIC - **Acurácia:** os valores fazem sentido para o negócio?
# MAGIC - **Outliers:** há valores extremos que podem distorcer as análises?

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType
import pandas as pd

CATALOG       = "workspace"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD   = "gold"

df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.inventory_raw")
df_silver = spark.table(f"{CATALOG}.{SCHEMA_SILVER}.inventory_clean")
df_fato   = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")

print(f"Bronze: {df_bronze.count():,} registros")
print(f"Silver: {df_silver.count():,} registros")
print(f"Fato  : {df_fato.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Completude: Valores Nulos

# COMMAND ----------

def relatorio_nulos(df, nome_tabela):
    total = df.count()
    print(f"\n=== {nome_tabela} | Total: {total:,} registros ===")
    for c in df.columns:
        n_nulos = df.filter(F.col(c).isNull()).count()
        pct = round(n_nulos * 100 / total, 2) if total > 0 else 0
        status = "OK" if n_nulos == 0 else "ATENCAO"
        print(f"  [{status}] {c:35s} → {n_nulos:6,} nulos ({pct}%)")

relatorio_nulos(df_bronze, "BRONZE: inventory_raw")
relatorio_nulos(df_silver, "SILVER: inventory_clean")
relatorio_nulos(df_fato,   "GOLD: fato_estoque_diario")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Unicidade: Duplicatas

# COMMAND ----------

print("=== SILVER: verificação de duplicatas na chave loja_id + produto_id + data ===")
total_silver  = df_silver.count()
distinct_keys = df_silver.select("loja_id", "produto_id", "data").distinct().count()
duplicatas    = total_silver - distinct_keys

print(f"  Total de registros   : {total_silver:,}")
print(f"  Chaves únicas        : {distinct_keys:,}")
print(f"  Duplicatas detectadas: {duplicatas:,}")

if duplicatas == 0:
    print("  [OK] Nenhuma duplicata encontrada.")
else:
    print("  [ATENCAO] Existem duplicatas. Verifique e trate antes de prosseguir.")
    df_silver.groupBy("loja_id", "produto_id", "data") \
             .count() \
             .filter(F.col("count") > 1) \
             .orderBy(F.col("count").desc()) \
             .show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consistência: Formatos e Domínios

# COMMAND ----------

print("=== Faixa de datas disponíveis ===")
df_silver.select(
    F.min("data").alias("data_minima"),
    F.max("data").alias("data_maxima")
).show()

print("=== Categorias únicas ===")
df_silver.select("categoria").distinct().orderBy("categoria").show(truncate=False)

print("=== Regiões únicas ===")
df_silver.select("regiao").distinct().orderBy("regiao").show(truncate=False)

print("=== Condições climáticas únicas ===")
df_silver.select("condicao_climatica").distinct().orderBy("condicao_climatica").show(truncate=False)

print("=== Sazonalidade únicas ===")
df_silver.select("sazonalidade").distinct().orderBy("sazonalidade").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Acurácia: Valores Impossíveis

# COMMAND ----------

print("=== SILVER: registros com estoque negativo ===")
neg_estoque = df_silver.filter(F.col("nivel_estoque") < 0)
print(f"  Encontrados: {neg_estoque.count()}")
neg_estoque.show(5)

print("\n=== SILVER: registros com vendas negativas ===")
neg_vendas = df_silver.filter(F.col("unidades_vendidas") < 0)
print(f"  Encontrados: {neg_vendas.count()}")
neg_vendas.show(5)

print("\n=== SILVER: registros com preço zero ou negativo ===")
preco_inv = df_silver.filter(F.col("preco_unitario") <= 0)
print(f"  Encontrados: {preco_inv.count()}")
preco_inv.show(5)

print("\n=== GOLD: registros com dias_cobertura negativo ===")
cobertura_neg = df_fato.filter(F.col("dias_cobertura") < 0)
print(f"  Encontrados: {cobertura_neg.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Outliers: Valores Extremos

# COMMAND ----------

print("=== Estatísticas descritivas das colunas numéricas (Silver) ===")
df_silver.select(
    "nivel_estoque",
    "unidades_vendidas",
    "preco_unitario",
    "desconto",
    "previsao_demanda"
).describe().show()

# COMMAND ----------

print("=== Estatísticas de dias_cobertura (Gold) ===")
df_fato.select("dias_cobertura", "demanda_media_7d", "nivel_estoque").describe().show()

# COMMAND ----------

print("=== Percentis de nivel_estoque ===")
df_silver.select(
    F.percentile_approx("nivel_estoque", [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).alias("percentis_estoque"),
    F.percentile_approx("unidades_vendidas", [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99]).alias("percentis_vendas")
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Distribuição da Flag de Estoque Crítico

# COMMAND ----------

print("=== Distribuição geral da flag de estoque crítico ===")
df_fato.groupBy("flag_estoque_critico") \
    .agg(
        F.count("*").alias("registros"),
        F.round(F.count("*") * 100 / df_fato.count(), 2).alias("percentual")
    ) \
    .show()

print("=== Dias de cobertura: distribuição ===")
df_fato.select(
    F.when(F.col("dias_cobertura") < 7,  "< 7 dias (critico)")
     .when(F.col("dias_cobertura") < 14, "7-14 dias (atencao)")
     .when(F.col("dias_cobertura") < 30, "14-30 dias (ok)")
     .otherwise("> 30 dias (folgado)").alias("faixa_cobertura")
).groupBy("faixa_cobertura").count().orderBy("faixa_cobertura").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo da Qualidade de Dados
# MAGIC
# MAGIC | Dimensão | Resultado | Ação tomada |
# MAGIC |---|---|---|
# MAGIC | Completude | Verificado por coluna | Nulos em colunas essenciais: linhas removidas. Nulos secundários: valor padrão |
# MAGIC | Unicidade | Verificado por chave natural | Duplicatas removidas na camada Silver |
# MAGIC | Consistência | Categorias e formatos verificados | Strings padronizadas (trim + uppercase) |
# MAGIC | Acurácia | Valores impossíveis verificados | Estoque/vendas negativos e preço zero removidos |
# MAGIC | Outliers | Percentis calculados | Dataset sintético, sem necessidade de remoção adicional |
# MAGIC
# MAGIC **Próximo passo:** Notebook `05_analise_negocio` para responder as perguntas de negócio.
