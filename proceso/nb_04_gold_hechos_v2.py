# Databricks notebook source
# MAGIC %md
# MAGIC # NB 04 - Capa Gold: Tabla de Hechos `fac_ecv`
# MAGIC
# MAGIC **Proyecto:** Análisis de Encuesta de Calidad de Vida (ECV) - DANE Colombia
# MAGIC **Capa:** Gold (Load) — Tabla de Hechos
# MAGIC **Autor:** Eduar Alonso Caro Montoya
# MAGIC
# MAGIC ## Objetivo
# MAGIC Construir la tabla de hechos central del modelo estrella, conectando las 5 dimensiones
# MAGIC mediante surrogate keys y calculando las métricas que alimentan los dashboards y KPIs
# MAGIC del proyecto original 2021.
# MAGIC
# MAGIC ## Modelo estrella resultante
# MAGIC
# MAGIC ```
# MAGIC                            ┌─────────────────┐
# MAGIC                            │   fac_ecv        │
# MAGIC                            │                  │
# MAGIC   ┌─────────────────┐      │  sk_persona  ◄───┼──► dim_persona
# MAGIC   │   dim_tiempo    │◄─────┼─ sk_tiempo       │
# MAGIC   └─────────────────┘      │  sk_vivienda ◄───┼──► dim_vivienda
# MAGIC                            │  sk_educacion◄───┼──► dim_educacion
# MAGIC   ┌─────────────────┐      │  sk_ubicacion◄───┼──► dim_ubicacion
# MAGIC   │  dim_ubicacion  │◄─────┤                  │
# MAGIC   └─────────────────┘      │  + métricas      │
# MAGIC                            └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Grano de la tabla
# MAGIC **Una fila por persona × año**. Cada fila representa "la situación de una persona
# MAGIC específica en un año específico de encuesta", con sus FKs a su vivienda, su registro
# MAGIC educativo, su ubicación geográfica y el año.
# MAGIC
# MAGIC ## Métricas calculadas
# MAGIC
# MAGIC | Métrica            | Definición                                    | KPI del proyecto |
# MAGIC |--------------------|-----------------------------------------------|------------------|
# MAGIC | total_personas     | Conteo de personas (constante = 1 por fila)   | Base poblacional |
# MAGIC | total_estudiante   | 1 si la persona estudia actualmente           | Margen Cobertura |
# MAGIC | total_becados      | 1 si recibió beca                             | Margen Cobertura |
# MAGIC | total_subsidiados  | 1 si recibió subsidio                         | Apoyo educativo  |
# MAGIC | total_credito      | 1 si recibió crédito educativo                | Apoyo educativo  |
# MAGIC | total_beca         | Valor monetario de la beca recibida           | Total invertido  |
# MAGIC | total_subsidio     | Valor monetario del subsidio                  | Total invertido  |
# MAGIC | total_credito_val  | Valor monetario del crédito                   | Total invertido  |
# MAGIC | total_en_riesgo    | 1 si la vivienda está en riesgo de desastre   | Condiciones Vida |
# MAGIC | total_sin_servicios| 1 si la vivienda no tiene servicios básicos   | Condiciones Vida |
# MAGIC | factor_expansion   | Peso muestral del DANE (para análisis pob.)   | Expansión        |
# MAGIC
# MAGIC ## Tablas agregadas adicionales
# MAGIC
# MAGIC Para acelerar dashboards en Power BI, se crean además 2 tablas pre-agregadas:
# MAGIC - `gold.kpi_becas_anuales` — KPIs de becas por año (Margen, Crecimiento)
# MAGIC - `gold.kpi_condiciones_vida_region` — KPIs de vivienda por región y año

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, DecimalType, TimestampType
)
from datetime import datetime, timezone
import uuid


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod"], "Entorno")
entorno = dbutils.widgets.get("entorno")
catalog_name = f"ecv_{entorno}"
run_id = str(uuid.uuid4())

print(f"Run ID  : {run_id}")
print(f"Catálogo: {catalog_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Utilidades

# COMMAND ----------

def registrar_auditoria(notebook, capa, tabla, inicio, registros_in, registros_out, estado, mensaje=""):
    fin = utc_now()
    duracion = (fin - inicio).total_seconds()
    audit_df = spark.createDataFrame(
        [(run_id, notebook, capa, tabla, registros_in, registros_out,
          estado, mensaje, inicio, fin, duracion)],
        schema=StructType([
            StructField("run_id", StringType()),
            StructField("notebook", StringType()),
            StructField("capa", StringType()),
            StructField("tabla_destino", StringType()),
            StructField("registros_in", LongType()),
            StructField("registros_out", LongType()),
            StructField("estado", StringType()),
            StructField("mensaje", StringType()),
            StructField("inicio_utc", TimestampType()),
            StructField("fin_utc", TimestampType()),
            StructField("duracion_seg", DoubleType()),
        ])
    )
    audit_df.write.format("delta").mode("append").saveAsTable(f"{catalog_name}.audit.pipeline_runs")


def generar_sk(columnas: list, alias: str = "sk"):
    """Surrogate key determinista vía SHA-256 truncado a 15 hex chars (BIGINT-safe)."""
    expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columnas])
    return F.conv(F.substring(F.sha2(expr, 256), 1, 15), 16, 10).cast(LongType()).alias(alias)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carga de Silver — la base del fact
# MAGIC
# MAGIC Partimos de `silver.persona` porque tiene el grano correcto (1 persona × año)
# MAGIC y ya viene unificada con educación y propagada con datos de vivienda.

# COMMAND ----------

df_persona  = spark.table(f"{catalog_name}.silver.persona")
df_vivienda = spark.table(f"{catalog_name}.silver.vivienda")
df_educ     = spark.table(f"{catalog_name}.silver.educacion")

print(f"silver.persona  : {df_persona.count():>10} filas")
print(f"silver.vivienda : {df_vivienda.count():>10} filas")
print(f"silver.educacion: {df_educ.count():>10} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construcción del fact con FKs y métricas
# MAGIC
# MAGIC Estrategia:
# MAGIC 1. Partir de silver.persona (grano: persona × año)
# MAGIC 2. Reconstruir las mismas surrogate keys que las dimensiones usaron (deterministas)
# MAGIC 3. Calcular las métricas a partir de las columnas booleanas y numéricas de Silver
# MAGIC 4. Filtrar y validar antes de escribir

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.fac_ecv"

try:
    # ── Trazas naturales para reconstruir SKs ─────────────────────────────
    # NOTA TÉCNICA: La clave natural de la vivienda es (directorio, anio_encuesta).
    # 'secuencia_encuesta' identifica el HOGAR dentro de la vivienda (1..24),
    # NO la vivienda física. Una vivienda puede contener varios hogares.
    claves_persona  = ["directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta"]
    claves_vivienda = ["directorio", "anio_encuesta"]

    # ── Tomar las columnas necesarias de persona (que ya incluye educación) ──
    df_base = df_persona.select(
        *claves_persona,

        # Para FK educación
        "actualmente_estudia",
        "recibio_beca", "valor_beca",
        "recibio_subsidio", "valor_subsidio",
        "recibio_credito", "valor_credito",
        "recibio_apoyo_educativo",

        # Para análisis demográfico (ya en dim_persona pero útil aquí)
        "factor_expansion",

        # Para FK ubicación
        "region",
    )

    # ── Agregar columnas booleanas de vivienda mediante JOIN ──
    df_viv_subset = df_vivienda.select(
        *claves_vivienda,
        "vivienda_en_riesgo",
        "sin_servicios_basicos",
    )

    df_fact = df_base.join(df_viv_subset, on=claves_vivienda, how="left")

    # ── Generar Surrogate Keys (mismos algoritmos que las dimensiones) ──
    df_fact = (df_fact
        .withColumn("sk_persona",   generar_sk(claves_persona))
        .withColumn("sk_vivienda",  generar_sk(claves_vivienda))
        .withColumn("sk_tiempo",    F.col("anio_encuesta").cast(IntegerType()))
    )

    # ── sk_educacion: NULL si la persona NO tiene registro educativo ──
    # Los menores de 5 años no están en el archivo Educación del DANE.
    # Hacemos lookup explícito contra dim_educacion en lugar de generar la SK ciegamente.
    df_dim_edu_lookup = spark.table(f"{catalog_name}.gold.dim_educacion").select(
        F.col("sk_educacion").alias("sk_educacion_lookup"),
        F.col("directorio").alias("edu_directorio"),
        F.col("secuencia_encuesta").alias("edu_secuencia_encuesta"),
        F.col("secuencia_p").alias("edu_secuencia_p"),
        F.col("orden").alias("edu_orden"),
        F.col("anio_encuesta").alias("edu_anio_encuesta"),
    )
    df_fact = (df_fact
        .join(df_dim_edu_lookup,
              (df_fact.directorio         == df_dim_edu_lookup.edu_directorio) &
              (df_fact.secuencia_encuesta == df_dim_edu_lookup.edu_secuencia_encuesta) &
              (df_fact.secuencia_p        == df_dim_edu_lookup.edu_secuencia_p) &
              (df_fact.orden              == df_dim_edu_lookup.edu_orden) &
              (df_fact.anio_encuesta      == df_dim_edu_lookup.edu_anio_encuesta),
              "left")
        .drop("edu_directorio", "edu_secuencia_encuesta", "edu_secuencia_p", "edu_orden", "edu_anio_encuesta")
        .withColumnRenamed("sk_educacion_lookup", "sk_educacion")
    )

    # ── FK a dim_ubicacion: lookup por nombre de región ──
    # Usamos LEFT JOIN porque años sin REGION (2017) deben quedar con sk_ubicacion NULL
    df_dim_ubi = spark.table(f"{catalog_name}.gold.dim_ubicacion").select(
        F.col("sk_ubicacion").alias("sk_ubicacion_lookup"),
        F.col("region").alias("region_lookup"),
    )
    df_fact = (df_fact
        .join(df_dim_ubi, df_fact.region == df_dim_ubi.region_lookup, "left")
        .drop("region_lookup")
        .withColumnRenamed("sk_ubicacion_lookup", "sk_ubicacion")
    )

    # ── Calcular métricas ──
    df_fact = (df_fact
        # Conteos (1 o 0 según el flag booleano)
        .withColumn("total_personas",     F.lit(1).cast(IntegerType()))
        .withColumn("total_estudiante",   F.coalesce(F.col("actualmente_estudia").cast("int"),     F.lit(0)))
        .withColumn("total_becados",      F.coalesce(F.col("recibio_beca").cast("int"),            F.lit(0)))
        .withColumn("total_subsidiados",  F.coalesce(F.col("recibio_subsidio").cast("int"),        F.lit(0)))
        .withColumn("total_credito",      F.coalesce(F.col("recibio_credito").cast("int"),         F.lit(0)))
        .withColumn("total_apoyo",        F.coalesce(F.col("recibio_apoyo_educativo").cast("int"), F.lit(0)))
        .withColumn("total_en_riesgo",    F.coalesce(F.col("vivienda_en_riesgo").cast("int"),      F.lit(0)))
        .withColumn("total_sin_servicios",F.coalesce(F.col("sin_servicios_basicos").cast("int"),   F.lit(0)))

        # Valores monetarios (en COP)
        .withColumn("monto_beca",     F.coalesce(F.col("valor_beca"),     F.lit(0).cast(DecimalType(14, 2))))
        .withColumn("monto_subsidio", F.coalesce(F.col("valor_subsidio"), F.lit(0).cast(DecimalType(14, 2))))
        .withColumn("monto_credito",  F.coalesce(F.col("valor_credito"),  F.lit(0).cast(DecimalType(14, 2))))
    )

    # ── Seleccionar columnas finales del fact ──
    df_fact_final = df_fact.select(
        # Claves naturales (trazabilidad)
        "directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta",

        # Surrogate Keys (FK a dimensiones)
        "sk_persona", "sk_vivienda", "sk_educacion", "sk_ubicacion", "sk_tiempo",

        # Métricas conteo
        "total_personas", "total_estudiante", "total_becados", "total_subsidiados",
        "total_credito", "total_apoyo", "total_en_riesgo", "total_sin_servicios",

        # Métricas monetarias
        "monto_beca", "monto_subsidio", "monto_credito",

        # Factor de expansión (para análisis poblacional ponderado)
        "factor_expansion",
    ).dropDuplicates(["sk_persona"])  # garantizar 1 fila por persona × año

    (df_fact_final.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    registros = df_fact_final.count()
    print(f"OK fac_ecv: {registros} filas")
    registrar_auditoria("nb_04_gold_hechos", "gold", "fac_ecv",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR fac_ecv: {msg}")
    registrar_auditoria("nb_04_gold_hechos", "gold", "fac_ecv",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación de integridad referencial
# MAGIC
# MAGIC Confirmar que las FKs del fact apuntan a registros existentes en cada dimensión.
# MAGIC En un modelo Kimball correcto, **toda FK debe encontrar match en su dimensión**
# MAGIC (excepto sk_ubicacion que puede ser NULL para años sin REGION).

# COMMAND ----------

print("=" * 70)
print("VERIFICACIÓN DE INTEGRIDAD REFERENCIAL")
print("=" * 70)

df_fact = spark.table(f"{catalog_name}.gold.fac_ecv")
total_fact = df_fact.count()

verificaciones = [
    ("sk_persona",   "dim_persona",   "sk_persona",   False),  # no debe haber NULL
    ("sk_vivienda",  "dim_vivienda",  "sk_vivienda",  False),  # no debe haber NULL
    ("sk_educacion", "dim_educacion", "sk_educacion", True),   # puede NULL (menores de 5 años)
    ("sk_tiempo",    "dim_tiempo",    "sk_tiempo",    False),  # no debe haber NULL
    ("sk_ubicacion", "dim_ubicacion", "sk_ubicacion", True),   # puede NULL (años sin REGION)
]

for fk_col, dim_tabla, dim_sk, permite_null in verificaciones:
    df_dim = spark.table(f"{catalog_name}.gold.{dim_tabla}").select(F.col(dim_sk).alias("sk_ref"))

    # Conteo de FKs no nulas que NO encuentran match en la dimensión
    df_match = (df_fact
        .filter(F.col(fk_col).isNotNull())
        .join(df_dim, df_fact[fk_col] == df_dim.sk_ref, "left_anti"))
    no_match = df_match.count()

    nulos = df_fact.filter(F.col(fk_col).isNull()).count()
    pct_nulos = (nulos * 100.0 / total_fact) if total_fact > 0 else 0

    estado_match = "OK" if no_match == 0 else "ALERTA"
    estado_null = "OK" if (permite_null or nulos == 0) else "ALERTA"

    print(f"  {fk_col:15s}: sin_match={no_match:>5} ({estado_match}), nulos={nulos:>6} ({pct_nulos:>5.1f}%) ({estado_null})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. KPI agregado: Margen de Cobertura de Becas por año
# MAGIC
# MAGIC Reproduce el KPI principal del proyecto 2021 (Figura 23 del PDF original):
# MAGIC
# MAGIC ```
# MAGIC Margen Cobertura Becas = Total Becados / Total Estudiantes
# MAGIC ```
# MAGIC
# MAGIC Para Power BI: esta tabla pre-agregada hace que el dashboard sea instantáneo.

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.kpi_becas_anuales"

try:
    df_fact = spark.table(f"{catalog_name}.gold.fac_ecv")

    df_kpi_becas = (df_fact
        .groupBy("anio_encuesta")
        .agg(
            F.sum("total_personas").alias("total_personas"),
            F.sum("total_estudiante").alias("total_estudiantes"),
            F.sum("total_becados").alias("total_becados"),
            F.sum("total_subsidiados").alias("total_subsidiados"),
            F.sum("total_credito").alias("total_credito"),
            F.sum("total_apoyo").alias("total_con_apoyo"),
            F.sum("monto_beca").alias("monto_total_becas"),
            F.sum("monto_subsidio").alias("monto_total_subsidios"),
            F.sum("monto_credito").alias("monto_total_creditos"),
        )
        .withColumn("margen_cobertura_becas_pct",
                    F.round(F.col("total_becados") * 100.0 / F.col("total_estudiantes"), 2))
        .withColumn("margen_cobertura_subsidios_pct",
                    F.round(F.col("total_subsidiados") * 100.0 / F.col("total_estudiantes"), 2))
        .withColumn("margen_cobertura_apoyo_total_pct",
                    F.round(F.col("total_con_apoyo") * 100.0 / F.col("total_estudiantes"), 2))
        .orderBy("anio_encuesta")
    )

    # Calcular crecimiento año contra año (función ventana)
    from pyspark.sql.window import Window
    w = Window.orderBy("anio_encuesta")
    df_kpi_becas = df_kpi_becas.withColumn(
        "crecimiento_becas_pct",
        F.round(
            (F.col("total_becados") - F.lag("total_becados").over(w)) * 100.0 /
            F.lag("total_becados").over(w),
            2
        )
    )

    (df_kpi_becas.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))

    registros = df_kpi_becas.count()
    print(f"OK kpi_becas_anuales: {registros} años")
    registrar_auditoria("nb_04_gold_hechos", "gold", "kpi_becas_anuales",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR kpi_becas_anuales: {msg}")
    registrar_auditoria("nb_04_gold_hechos", "gold", "kpi_becas_anuales",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. KPI agregado: Condiciones de Vida por Región
# MAGIC
# MAGIC Reproduce el dashboard de "Condiciones de Vida" del proyecto original (Figura 22 PDF):
# MAGIC - Viviendas en riesgo de inundación/deslizamiento/hundimiento
# MAGIC - Viviendas sin servicios básicos
# MAGIC
# MAGIC Solo aplica para años con REGION (2018+).

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.kpi_condiciones_vida_region"

try:
    df_fact = spark.table(f"{catalog_name}.gold.fac_ecv")
    df_dim_ubi = spark.table(f"{catalog_name}.gold.dim_ubicacion")

    # JOIN con dim_ubicacion para tener el nombre de la región disponible
    df_kpi_vida = (df_fact
        .filter(F.col("sk_ubicacion").isNotNull())
        .join(df_dim_ubi, on="sk_ubicacion", how="inner")
        .groupBy("anio_encuesta", "region_id", "region")
        .agg(
            F.sum("total_personas").alias("total_personas"),
            F.sum("total_en_riesgo").alias("personas_en_vivienda_riesgo"),
            F.sum("total_sin_servicios").alias("personas_sin_servicios_basicos"),
        )
        .withColumn("pct_en_riesgo",
                    F.round(F.col("personas_en_vivienda_riesgo") * 100.0 / F.col("total_personas"), 2))
        .withColumn("pct_sin_servicios",
                    F.round(F.col("personas_sin_servicios_basicos") * 100.0 / F.col("total_personas"), 2))
        .orderBy("anio_encuesta", "region_id")
    )

    (df_kpi_vida.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))

    registros = df_kpi_vida.count()
    print(f"OK kpi_condiciones_vida_region: {registros} filas")
    registrar_auditoria("nb_04_gold_hechos", "gold", "kpi_condiciones_vida_region",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR kpi_condiciones_vida_region: {msg}")
    registrar_auditoria("nb_04_gold_hechos", "gold", "kpi_condiciones_vida_region",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen de la corrida

# COMMAND ----------

df_resumen = (
    spark.table(f"{catalog_name}.audit.pipeline_runs")
    .filter(F.col("run_id") == run_id)
    .select("tabla_destino", "estado", "registros_out", "duracion_seg")
    .orderBy("inicio_utc")
)
display(df_resumen)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Visualización de los KPIs

# COMMAND ----------

print("=" * 70)
print("KPI: Margen de Cobertura de Becas por Año")
print("=" * 70)
print("Reproduce Figura 23 del proyecto original 2021")
print()
spark.table(f"{catalog_name}.gold.kpi_becas_anuales").select(
    "anio_encuesta",
    "total_estudiantes",
    "total_becados",
    "margen_cobertura_becas_pct",
    "crecimiento_becas_pct",
).show(truncate=False)

# COMMAND ----------

print("=" * 70)
print("KPI: Apoyos Educativos Totales por Año (Becas + Subsidios + Créditos)")
print("=" * 70)
spark.table(f"{catalog_name}.gold.kpi_becas_anuales").select(
    "anio_encuesta",
    "total_becados", "total_subsidiados", "total_credito",
    "total_con_apoyo",
    "margen_cobertura_apoyo_total_pct",
).show(truncate=False)

# COMMAND ----------

print("=" * 70)
print("KPI: Condiciones de Vida por Región (solo años con REGION)")
print("=" * 70)
print("Reproduce el dashboard 'Condiciones de Vida' del proyecto original")
print()
spark.table(f"{catalog_name}.gold.kpi_condiciones_vida_region").select(
    "anio_encuesta", "region",
    "total_personas",
    "personas_en_vivienda_riesgo", "pct_en_riesgo",
    "personas_sin_servicios_basicos", "pct_sin_servicios",
).orderBy("anio_encuesta", F.desc("pct_en_riesgo")).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Consultas analíticas de prueba
# MAGIC
# MAGIC Demuestra que el modelo dimensional soporta preguntas complejas con joins directos.

# COMMAND ----------

print("=" * 70)
print("CONSULTA 1: Cobertura de becas por nivel educativo (con join a dim_educacion)")
print("=" * 70)

df_fact     = spark.table(f"{catalog_name}.gold.fac_ecv")
df_dim_edu  = spark.table(f"{catalog_name}.gold.dim_educacion")

(df_fact
    .join(df_dim_edu, on="sk_educacion", how="inner")
    .filter(F.col("actualmente_estudia") == True)
    .groupBy("nivel_educativo")
    .agg(
        F.sum("total_estudiante").alias("estudiantes"),
        F.sum("total_becados").alias("becados"),
    )
    .withColumn("cobertura_pct", F.round(F.col("becados") * 100.0 / F.col("estudiantes"), 2))
    .orderBy(F.desc("cobertura_pct"))
    .show(15, truncate=False))

# COMMAND ----------

print("=" * 70)
print("CONSULTA 2: Migrantes en cada región (join fact + persona + ubicacion)")
print("=" * 70)
print("Solo años con REGION disponible")
print()

df_dim_per  = spark.table(f"{catalog_name}.gold.dim_persona")
df_dim_ubi  = spark.table(f"{catalog_name}.gold.dim_ubicacion")

(df_fact
    .filter(F.col("sk_ubicacion").isNotNull())
    .join(df_dim_per, on="sk_persona", how="inner")
    .join(df_dim_ubi, on="sk_ubicacion", how="inner")
    .groupBy("region")
    .agg(
        F.count("*").alias("total_personas"),
        F.sum(F.col("es_migrante").cast("int")).alias("migrantes"),
        F.sum(F.col("nacio_en_otro_pais").cast("int")).alias("extranjeros"),
    )
    .withColumn("pct_migrantes", F.round(F.col("migrantes") * 100.0 / F.col("total_personas"), 2))
    .orderBy(F.desc("pct_migrantes"))
    .show(truncate=False))

# COMMAND ----------

print("=" * 70)
print("CONSULTA 3: Inversión total en apoyos educativos por año (en millones COP)")
print("=" * 70)

(spark.table(f"{catalog_name}.gold.kpi_becas_anuales")
    .select(
        "anio_encuesta",
        F.round(F.col("monto_total_becas")     / 1_000_000, 2).alias("becas_millones"),
        F.round(F.col("monto_total_subsidios") / 1_000_000, 2).alias("subsidios_millones"),
        F.round(F.col("monto_total_creditos")  / 1_000_000, 2).alias("creditos_millones"),
    )
    .show(truncate=False))

# COMMAND ----------

dbutils.notebook.exit(f"GOLD_FACT_OK run_id={run_id}")
