# Databricks notebook source
# MAGIC %md
# MAGIC # NB 03 - Capa Gold: Dimensiones del modelo estrella
# MAGIC
# MAGIC **Proyecto:** Análisis de Encuesta de Calidad de Vida (ECV) - DANE Colombia
# MAGIC **Capa:** Gold (Load) — Dimensiones
# MAGIC **Autor:** Eduar Alonso Caro Montoya
# MAGIC
# MAGIC ## Objetivo
# MAGIC Construir las dimensiones del modelo estrella, evolucionando el modelo del cubo SSAS, con las correcciones identificadas
# MAGIC durante la validación contra el diccionario oficial DANE.
# MAGIC
# MAGIC ## Modelo dimensional resultante
# MAGIC
# MAGIC | Tabla Gold       | Origen Silver           | Grano                            |
# MAGIC |------------------|-------------------------|----------------------------------|
# MAGIC | dim_persona      | silver.persona          | 1 persona × año                  |
# MAGIC | dim_vivienda     | silver.vivienda         | 1 vivienda × año                 |
# MAGIC | dim_educacion    | silver.educacion        | 1 registro educación × año       |
# MAGIC | dim_ubicacion    | silver.vivienda.region  | 1 fila por región (filtra NULL)  |
# MAGIC | dim_tiempo       | silver.persona.anio     | 1 año                            |
# MAGIC
# MAGIC ## Decisiones de diseño clave
# MAGIC
# MAGIC ### dim_persona desnormalizada (Kimball clásico)
# MAGIC En lugar de crear `dim_departamento` y `dim_municipio` separadas (que generaría
# MAGIC snowflaking), los atributos del lugar de nacimiento quedan denormalizados en
# MAGIC `dim_persona` con sus nombres ya decodificados vía DIVIPOLA en Silver.
# MAGIC
# MAGIC ### Surrogate Keys (SK) deterministas
# MAGIC SHA-256 truncado a 15 caracteres hexadecimales convertidos a BIGINT.
# MAGIC - **15 caracteres** (no 16): garantiza compatibilidad con BIGINT signed de Spark
# MAGIC   (2^60 valores únicos disponibles, más que suficiente)
# MAGIC - **Deterministas**: mismas claves naturales producen siempre el mismo SK
# MAGIC - **Reproducibles**: re-ejecutar el notebook produce exactamente las mismas SKs
# MAGIC
# MAGIC ### Estrategia de escritura
# MAGIC `overwrite` con `overwriteSchema=true`: el notebook es idempotente, se puede
# MAGIC re-ejecutar sin generar duplicados.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, TimestampType
)
from datetime import datetime, timezone
import uuid


def utc_now() -> datetime:
    """Reemplazo tz-aware de datetime.utcnow() (deprecada en Python 3.12+)."""
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
    """Surrogate key determinista vía SHA-256 sobre concatenación de columnas.

    Decisiones técnicas:
    - SHA-256 garantiza distribución uniforme y resistencia a colisiones
    - Tomamos 15 hex chars (no 16) porque BIGINT en Spark es SIGNED:
        * 16 hex → hasta 2^64-1, puede exceder 2^63-1 y fallar el cast
        * 15 hex → hasta 2^60 = 1.15e18, cabe siempre en BIGINT
    - Determinístico: mismas columnas → mismo SK siempre (idempotente)
    - NULL se mapea a la cadena literal 'NULL' para que sea reproducible
    """
    expr = F.concat_ws("||", *[F.coalesce(F.col(c).cast("string"), F.lit("NULL")) for c in columnas])
    return F.conv(F.substring(F.sha2(expr, 256), 1, 15), 16, 10).cast(LongType()).alias(alias)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_tiempo
# MAGIC
# MAGIC Una fila por año presente en los datos. SK = año (entero), facilita debugging.

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.dim_tiempo"

try:
    df_persona = spark.table(f"{catalog_name}.silver.persona")
    anios = df_persona.select("anio_encuesta").distinct().filter(F.col("anio_encuesta").isNotNull())

    anio_actual = datetime.now().year

    df_dim_tiempo = (anios
        .withColumnRenamed("anio_encuesta", "anio")
        .withColumn("sk_tiempo", F.col("anio").cast(IntegerType()))
        .withColumn("es_anio_actual", F.col("anio") == F.lit(anio_actual))
        .withColumn("decada", F.concat(F.substring(F.col("anio").cast("string"), 1, 3), F.lit("0s")))
        .select("sk_tiempo", "anio", "decada", "es_anio_actual")
        .orderBy("anio")
    )

    (df_dim_tiempo.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))

    registros = df_dim_tiempo.count()
    print(f"OK dim_tiempo: {registros} años")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_tiempo",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR dim_tiempo: {msg}")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_tiempo",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_ubicacion
# MAGIC
# MAGIC Solo regiones con valor (no NULL). Para los años sin región (ej. 2017), los hechos
# MAGIC quedarán sin FK válida y se conectarán a un miembro "Sin información" si fuera necesario,
# MAGIC o simplemente la FK quedará NULL.
# MAGIC
# MAGIC ### Estructura
# MAGIC ```
# MAGIC sk_ubicacion : surrogate key
# MAGIC region_id    : código original DANE (1-9)
# MAGIC region       : nombre de la región
# MAGIC ```
# MAGIC
# MAGIC ### Las 9 regiones del DANE
# MAGIC Caribe, Oriental, Central, Pacífica (sin valle), Bogotá, Antioquia,
# MAGIC Valle del Cauca, San Andrés, Orinoquía-Amazonía.

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.dim_ubicacion"

try:
    df_viv = spark.table(f"{catalog_name}.silver.vivienda")

    # Obtener regiones distintas con sus códigos
    # Necesitamos extraer el código original (1-9) para conservar trazabilidad
    df_regiones = (df_viv
        .filter(F.col("region").isNotNull())
        .select("region")
        .distinct()
    )

    # Mapeo region -> region_id (orden oficial DANE)
    region_a_id = {
        "Caribe": 1, "Oriental": 2, "Central": 3,
        "Pacífica (sin valle)": 4, "Bogotá": 5, "Antioquia": 6,
        "Valle del Cauca": 7, "San Andrés": 8, "Orinoquía-Amazonía": 9,
    }

    mapping_expr = F.create_map(*[F.lit(x) for kv in region_a_id.items() for x in kv])

    df_dim_ubicacion = (df_regiones
        .withColumn("region_id", mapping_expr[F.col("region")])
        .withColumn("sk_ubicacion", generar_sk(["region"]))
        .select("sk_ubicacion", "region_id", "region")
        .orderBy("region_id")
    )

    (df_dim_ubicacion.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))

    registros = df_dim_ubicacion.count()
    print(f"OK dim_ubicacion: {registros} regiones")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_ubicacion",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR dim_ubicacion: {msg}")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_ubicacion",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_persona (desnormalizada con lugar de nacimiento)
# MAGIC
# MAGIC Esta es la dimensión más rica del modelo. Incluye:
# MAGIC - **Atributos demográficos**: sexo, edad, estado civil, raza
# MAGIC - **Satisfacciones**: 5 dimensiones de bienestar subjetivo (escala 0-10)
# MAGIC - **Lugar de nacimiento decodificado**: nombre depto/municipio vía DIVIPOLA
# MAGIC - **Flags de migración**: migrante interno, nacido en el extranjero
# MAGIC
# MAGIC La denormalización del lugar de nacimiento sigue mejores prácticas Kimball:
# MAGIC evita snowflaking y permite consultas analíticas directas como:
# MAGIC *"distribución de migrantes hacia región Bogotá por departamento de origen"*.

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.dim_persona"

try:
    df_persona = spark.table(f"{catalog_name}.silver.persona")

    claves_persona = ["directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta"]

    df_dim_persona = (df_persona
        .withColumn("sk_persona", generar_sk(claves_persona))
        .select(
            "sk_persona",

            # Claves naturales (para trazabilidad)
            "directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta",

            # Atributos demográficos
            "sexo", "edad", "rango_edad", "estado_civil", "raza",

            # Lugar de nacimiento (denormalizado desde DIVIPOLA)
            "lugar_nacimiento",
            "cod_dept_nacimiento", "nombre_dept_nacimiento",
            "cod_mun_nacimiento",  "nombre_mun_nacimiento",

            # Flags de migración (útiles para dashboards)
            "nacio_en_otro_pais", "nacio_en_otro_municipio", "es_migrante",

            # Bienestar subjetivo
            F.coalesce(F.col("satisfaccion_vida"),      F.lit(None).cast(IntegerType())).alias("satisfaccion_vida"),
            F.coalesce(F.col("satisfaccion_ingresos"),  F.lit(None).cast(IntegerType())).alias("satisfaccion_ingresos"),
            F.coalesce(F.col("satisfaccion_salud"),     F.lit(None).cast(IntegerType())).alias("satisfaccion_salud"),
            F.coalesce(F.col("satisfaccion_seguridad"), F.lit(None).cast(IntegerType())).alias("satisfaccion_seguridad"),
            F.coalesce(F.col("satisfaccion_trabajo"),   F.lit(None).cast(IntegerType())).alias("satisfaccion_trabajo"),

            # Factor de expansión (para análisis poblacional ponderado)
            "factor_expansion",
        )
        .dropDuplicates(["sk_persona"])
    )

    (df_dim_persona.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    registros = df_dim_persona.count()
    print(f"OK dim_persona: {registros} personas")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_persona",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR dim_persona: {msg}")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_persona",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_vivienda
# MAGIC
# MAGIC Características físicas + servicios públicos + riesgos por desastres + problemas estructurales.
# MAGIC La región se conserva como atributo aquí también para análisis directos (sin necesidad de
# MAGIC llegar al fact si el análisis es solo a nivel vivienda).

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.dim_vivienda"

try:
    df_viv = spark.table(f"{catalog_name}.silver.vivienda")

    claves_vivienda = ["directorio", "anio_encuesta"]

    df_dim_vivienda = (df_viv
        .withColumn("sk_vivienda", generar_sk(claves_vivienda))
        .select(
            "sk_vivienda",
            "directorio", "anio_encuesta",

            # Características físicas
            "tipo_vivienda", "material_paredes", "material_pisos", "material_techo",

            # Geografía (en años sin REGION, este campo es NULL)
            "clase_geografica", "region",

            # Servicios públicos
            "energia_electrica", "acueducto", "alcantarillado", "recoleccion_basuras",
            "sin_servicios_basicos",

            # Riesgos por desastres naturales (P4065S*)
            "riesgo_inundacion", "riesgo_avalancha", "riesgo_hundimiento", "riesgo_tormenta",
            "vivienda_en_riesgo",

            # Problemas estructurales (P1891S*)
            "problema_humedades", "problema_goteras", "problema_grietas_paredes",
            "problema_grietas_piso", "problema_cielorrasos",
        )
        .dropDuplicates(["sk_vivienda"])
    )

    (df_dim_vivienda.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    registros = df_dim_vivienda.count()
    print(f"OK dim_vivienda: {registros} viviendas")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_vivienda",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR dim_vivienda: {msg}")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_vivienda",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. dim_educacion
# MAGIC
# MAGIC Atributos educativos + 3 tipos de apoyos económicos separados según la documentación
# MAGIC oficial DANE (beca, subsidio, crédito son conceptos distintos, no se deben mezclar).

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.gold.dim_educacion"

try:
    df_edu = spark.table(f"{catalog_name}.silver.educacion")

    claves_educacion = ["directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta"]

    df_dim_educacion = (df_edu
        .withColumn("sk_educacion", generar_sk(claves_educacion))
        .select(
            "sk_educacion",
            "directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta",

            # Variables básicas
            "sabe_leer_escribir", "actualmente_estudia",
            "nivel_educativo", "grado_aprobado", "anios_estudios_superiores",
            "razon_no_estudiar",

            # Establecimiento (si estudia)
            "tipo_establecimiento", "jornada_educativa",

            # Apoyos económicos (3 conceptos separados según DANE)
            "recibio_beca",     "valor_beca",     "entidad_beca",
            "recibio_subsidio", "valor_subsidio",
            "recibio_credito",  "valor_credito",
            "recibio_apoyo_educativo",
        )
        .dropDuplicates(["sk_educacion"])
    )

    (df_dim_educacion.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    registros = df_dim_educacion.count()
    print(f"OK dim_educacion: {registros} registros")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_educacion",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR dim_educacion: {msg}")
    registrar_auditoria("nb_03_gold_dimensiones", "gold", "dim_educacion",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resumen y verificaciones de integridad

# COMMAND ----------

df_resumen = (
    spark.table(f"{catalog_name}.audit.pipeline_runs")
    .filter(F.col("run_id") == run_id)
    .select("tabla_destino", "estado", "registros_out", "duracion_seg")
    .orderBy("inicio_utc")
)
display(df_resumen)

# COMMAND ----------

print("=" * 70)
print("VERIFICACIÓN DE INTEGRIDAD DE SURROGATE KEYS")
print("=" * 70)
print("Cada SK debe ser única dentro de su dimensión. Una colisión sería bug grave.\n")

dimensiones = [
    ("dim_persona",   "sk_persona"),
    ("dim_vivienda",  "sk_vivienda"),
    ("dim_educacion", "sk_educacion"),
    ("dim_ubicacion", "sk_ubicacion"),
    ("dim_tiempo",    "sk_tiempo"),
]

for dim, sk_col in dimensiones:
    df = spark.table(f"{catalog_name}.gold.{dim}")
    total = df.count()
    unicos = df.select(sk_col).distinct().count()
    nulos = df.filter(F.col(sk_col).isNull()).count()
    estado = "OK" if (total == unicos and nulos == 0) else "ALERTA"
    print(f"  {dim:20s}: total={total:>10}, únicos={unicos:>10}, nulos={nulos:>3} -> {estado}")

# COMMAND ----------

print("\n=== Distribución por año de cada dimensión ===\n")
for dim in ["dim_persona", "dim_vivienda", "dim_educacion"]:
    print(f"{dim}:")
    spark.table(f"{catalog_name}.gold.{dim}") \
        .groupBy("anio_encuesta").count() \
        .orderBy("anio_encuesta").show()

# COMMAND ----------

print("\n=== Contenido completo de dim_tiempo ===")
spark.table(f"{catalog_name}.gold.dim_tiempo").orderBy("anio").show(truncate=False)

print("\n=== Contenido completo de dim_ubicacion ===")
spark.table(f"{catalog_name}.gold.dim_ubicacion").orderBy("region_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validaciones analíticas
# MAGIC
# MAGIC Pruebas de consultas que el modelo dimensional debe soportar.

# COMMAND ----------

print("=== Top 10 departamentos de origen de migrantes (todos los años) ===")
(spark.table(f"{catalog_name}.gold.dim_persona")
    .filter(F.col("es_migrante") == True)
    .filter(F.col("nombre_dept_nacimiento").isNotNull())
    .groupBy("nombre_dept_nacimiento").count()
    .orderBy("count", ascending=False)
    .show(10, truncate=False))

print("=== % de población migrante por año ===")
df_dp = spark.table(f"{catalog_name}.gold.dim_persona")
(df_dp.groupBy("anio_encuesta")
    .agg(
        F.count("*").alias("total"),
        F.sum(F.col("es_migrante").cast("int")).alias("migrantes"),
        F.sum(F.col("nacio_en_otro_pais").cast("int")).alias("extranjeros"),
    )
    .withColumn("pct_migrantes", F.round(F.col("migrantes") * 100.0 / F.col("total"), 2))
    .orderBy("anio_encuesta")
    .show(truncate=False))

print("=== Viviendas en riesgo por región (solo 2018+) ===")
(spark.table(f"{catalog_name}.gold.dim_vivienda")
    .filter(F.col("region").isNotNull())
    .groupBy("region")
    .agg(
        F.count("*").alias("total_viviendas"),
        F.sum(F.col("vivienda_en_riesgo").cast("int")).alias("en_riesgo"),
    )
    .withColumn("pct_riesgo", F.round(F.col("en_riesgo") * 100.0 / F.col("total_viviendas"), 2))
    .orderBy("pct_riesgo", ascending=False)
    .show(truncate=False))

print("=== KPI: Margen de Cobertura de Becas por año ===")
df_edu = spark.table(f"{catalog_name}.gold.dim_educacion")
(df_edu.groupBy("anio_encuesta")
    .agg(
        F.sum(F.col("actualmente_estudia").cast("int")).alias("estudiantes"),
        F.sum(F.col("recibio_beca").cast("int")).alias("becados"),
    )
    .withColumn("margen_cobertura_becas_pct",
                F.round(F.col("becados") * 100.0 / F.col("estudiantes"), 2))
    .orderBy("anio_encuesta")
    .show(truncate=False))

# COMMAND ----------

dbutils.notebook.exit(f"GOLD_DIM_OK run_id={run_id}")
