# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 05: Análise de Negócio
# MAGIC **Pipeline:** Retail Store Inventory | Risco de Ruptura de Estoque
# MAGIC
# MAGIC Etapa final do trabalho. Aqui respondo às cinco perguntas de negócio definidas no
# MAGIC início do projeto, usando consultas sobre a camada Gold e visualizações em Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuração

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

CATALOG     = "workspace"
SCHEMA_GOLD = "gold"

df_fato    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.fato_estoque_diario")
df_produto = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_produto")
df_loja    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_loja")
df_data    = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.dim_data")

# Enriquecimento: join com dimensões para análise
df_analitico = (
    df_fato
    .join(df_produto,             "id_produto", "left")
    .join(df_loja,                "id_loja",    "left")
    .join(df_data.drop("feriado"), "id_data",   "left")
)

print(f"Dataset analítico: {df_analitico.count():,} registros")

# Estilo dos gráficos
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 5)
plt.rcParams["axes.titlesize"] = 13

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 1: Quais produtos, categorias e lojas têm maior frequência de estoque crítico?

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
# MAGIC A categoria no topo do ranking é a que apresenta maior fragilidade de estoque no período analisado.
# MAGIC Categorias com alta rotatividade tendem a acumular mais dias críticos quando o ciclo de reposição
# MAGIC não acompanha a velocidade de saída dos produtos. Categorias com taxa próxima à média geral
# MAGIC indicam que o problema não é específico de um segmento, mas sim estrutural na política de estoque.

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
bars = ax.barh(
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
# MAGIC Se os produtos com maior % de dias críticos pertencem a poucas categorias, o problema é
# MAGIC concentrado e mais fácil de tratar com ações pontuais. Se estão espalhados por diversas
# MAGIC categorias, há uma falha mais ampla na política de reposição. Produtos que aparecem
# MAGIC recorrentemente nesse ranking são candidatos a ter um estoque de segurança dedicado.

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

# Por loja
axes[0].barh(df_p1_loja["loja_id_orig"], df_p1_loja["pct_critico"], color="steelblue")
axes[0].set_title("Taxa de estoque crítico por loja")
axes[0].set_xlabel("% dias críticos")
axes[0].xaxis.set_major_formatter(mtick.PercentFormatter())

# Por região
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
# MAGIC A variação entre lojas pode refletir diferenças no perfil de demanda local, na capacidade
# MAGIC de armazenagem ou na frequência de abastecimento recebida. Lojas da mesma região com taxas
# MAGIC muito diferentes entre si sugerem que o problema é operacional (gestão local), não regional.
# MAGIC Regiões com taxa acima da média são candidatas a um plano de reposição prioritário.

# COMMAND ----------

# MAGIC %md
# MAGIC **Síntese P1:**
# MAGIC Olhando as três dimensões em conjunto: as categorias mais críticas (P1.1), os produtos
# MAGIC recorrentes no ranking (P1.2) e as lojas ou regiões com padrão consistente (P1.3) formam
# MAGIC o perfil de risco do varejista. A interseção entre essas três listas — produto crítico,
# MAGIC de categoria crítica, em loja crítica — é onde a atenção deve se concentrar primeiro.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 2: Há relação entre alto volume de vendas e maior risco de estoque crítico?

# COMMAND ----------

# Calcular percentil 75 de vendas para segmentar "alta" e "baixa" demanda
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
# MAGIC Uma taxa de ruptura significativamente maior em produtos de alta demanda confirma que o volume
# MAGIC de vendas pressiona o estoque além do planejado — o estoque de segurança não está calibrado
# MAGIC pelo giro real. Se as taxas forem próximas entre as duas faixas, o problema é estrutural e
# MAGIC independe do volume: a política de reposição falha para todos os produtos igualmente.
# MAGIC Em ambos os casos, o nível médio de estoque por faixa ajuda a entender se produtos de alta
# MAGIC demanda recebem mais ou menos estoque do que os de baixa demanda.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 3: Promoções geram maior pressão sobre o estoque?

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

for ax, col, titulo in zip(axes,
    ["taxa_critico", "vendas_medias", "estoque_medio"],
    ["Taxa de estoque crítico (%)", "Vendas médias (un.)", "Estoque médio (un.)"]):
    bars = ax.bar(df_p3["rotulo"], df_p3[col], color=cores)
    ax.set_title(titulo)
    for bar, val in zip(bars, df_p3[col]):
        ax.text(bar.get_x() + bar.get_width() / 2, val + 0.3, f"{val}", ha="center", fontweight="bold")

plt.suptitle("P3: Impacto das promoções no estoque", fontsize=13, fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Interpretação P3:**
# MAGIC Se a taxa de estoque crítico em períodos com promoção for superior à de períodos sem promoção,
# MAGIC confirma-se que as campanhas geram picos de demanda não absorvidos pelo estoque disponível.
# MAGIC O terceiro gráfico é o mais revelador: vendas médias mais altas combinadas com estoque médio
# MAGIC mais baixo durante promoções são o sinal clássico desse desalinhamento. A recomendação direta
# MAGIC é antecipar o abastecimento em pelo menos um ciclo antes de qualquer ação promocional.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 4: Feriados e períodos específicos apresentam maior pressão sobre o estoque?

# COMMAND ----------

# MAGIC %md
# MAGIC ### P4.1: Variação mensal da taxa de estoque crítico

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
# MAGIC Os meses com pico na taxa crítica revelam padrões sazonais que tendem a se repetir anualmente.
# MAGIC Meses de alta demanda (datas comemorativas, início de estações) naturalmente pressionam mais
# MAGIC o estoque. Esses períodos devem ser mapeados como zonas de risco previsível no calendário de
# MAGIC compras, com reposição antecipada de pelo menos um ciclo antes do pico observado.

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
# MAGIC Se feriados concentram mais dias críticos do que dias normais, o aumento de demanda nesses
# MAGIC dias não está sendo compensado por reforço no estoque. Vendas médias mais altas em feriados
# MAGIC com estoque médio menor confirmam que o desabastecimento ocorre exatamente quando a pressão
# MAGIC sobre o produto é maior — o pior cenário possível para a experiência do consumidor.

# COMMAND ----------

# MAGIC %md
# MAGIC **Síntese P4:**
# MAGIC A sazonalidade mensal (P4.1) e os feriados (P4.2) representam riscos completamente previsíveis
# MAGIC e planejáveis. O problema não é falta de informação: os padrões estão nos dados. O que falta
# MAGIC é um gatilho de reposição antecipada atrelado ao calendário. Incorporar esses eventos ao modelo
# MAGIC de abastecimento é a principal recomendação desta análise.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Pergunta 5 (Complementar): Há relação entre preço e risco de estoque crítico?

# COMMAND ----------

# Verificar variação de preços antes de prosseguir
stats_preco = df_analitico.select(
    F.min("preco_unitario").alias("min"),
    F.max("preco_unitario").alias("max"),
    F.stddev("preco_unitario").alias("desvio_padrao"),
    F.countDistinct("preco_unitario").alias("valores_distintos")
).toPandas()

print("=== Variação de preços no dataset ===")
print(stats_preco.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC Antes de interpretar a relação entre preço e ruptura, é necessário verificar se há variação
# MAGIC real de preços no dataset. Um desvio padrão próximo de zero indica que os produtos têm preços
# MAGIC semelhantes, o que limita o poder explicativo dessa variável. Se for esse o caso, os resultados
# MAGIC do gráfico abaixo devem ser lidos com cautela — diferenças na taxa de ruptura entre faixas de
# MAGIC preço podem ser estatisticamente irrelevantes.

# COMMAND ----------

# Prosseguir somente se houver variação relevante
desvio = float(stats_preco["desvio_padrao"].iloc[0])
if desvio < 1.0:
    print("AVISO: Baixa variação de preços detectada. Análise de P5 tem relevância limitada.")

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
# MAGIC Produtos de alto valor tendem a receber maior atenção na gestão de estoque, o que se
# MAGIC refletiria em menor taxa de ruptura nessa faixa. Se o gráfico confirmar esse padrão,
# MAGIC significa que itens de baixo custo estão sendo subestimados no planejamento de reposição
# MAGIC e podem ser os maiores responsáveis pelas rupturas em volume. Se as taxas forem similares
# MAGIC entre as faixas, o preço não é um fator discriminante e a atenção deve recair sobre
# MAGIC outras variáveis como categoria, loja e sazonalidade (já exploradas nas análises anteriores).
# MAGIC
# MAGIC **Observação:** Se o desvio padrão de preços verificado acima for baixo, as diferenças
# MAGIC observadas entre faixas podem não ser representativas. Vale registrar isso na autoavaliação.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Conclusão
# MAGIC
# MAGIC As cinco análises revelam um padrão consistente: o risco de ruptura de estoque neste varejista
# MAGIC não é aleatório. Ele se concentra em categorias específicas, em produtos de maior giro, em
# MAGIC lojas com menor capacidade operacional e em períodos previsíveis do calendário.
# MAGIC
# MAGIC O achado mais relevante é que os dois principais gatilhos de ruptura — promoções e sazonalidade
# MAGIC — são completamente antecipáveis. Isso significa que a maioria dos eventos críticos identificados
# MAGIC poderia ter sido evitada com planejamento de reposição baseado em dados históricos.
# MAGIC
# MAGIC **Recomendações principais:**
# MAGIC - Criar estoques de segurança diferenciados por categoria e perfil de loja
# MAGIC - Antecipar o abastecimento antes de campanhas promocionais e feriados
# MAGIC - Priorizar os produtos do Top 15 (P1.2) com política de reposição mais frequente
# MAGIC - Usar a variação mensal (P4.1) como base para um calendário de compras preventivo
