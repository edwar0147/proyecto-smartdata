# Databricks notebook source
# MAGIC %md
# MAGIC # NB 05 - Serving Layer: Azure SQL Database
# MAGIC
# MAGIC **Proyecto:** Análisis de Encuesta de Calidad de Vida (ECV) - DANE Colombia
# MAGIC **Capa:** Serving — Sincronización Gold → Azure SQL
# MAGIC **Autor:** Eduar Alonso Caro Montoya
# MAGIC
# MAGIC ## Objetivo
# MAGIC Exponer las tablas Gold del Delta Lake hacia una base de datos Azure SQL para
# MAGIC consumo desde herramientas de BI como Power BI, Tableau, etc.
# MAGIC
# MAGIC ## Por qué un serving layer
# MAGIC
# MAGIC Aunque Power BI puede conectarse directamente a Databricks SQL Warehouse, hay
# MAGIC ventajas concretas de exponer los datos Gold a través de Azure SQL Database:
# MAGIC
# MAGIC 1. **Performance**: Azure SQL responde más rápido a consultas analíticas pequeñas
# MAGIC    que un cluster Databricks (sin tiempo de arranque del compute)
# MAGIC 2. **Independencia**: Power BI no necesita que el workspace Databricks esté activo
# MAGIC 3. **Compatibilidad universal**: cualquier herramienta BI (incluso legacy) conecta
# MAGIC    a Azure SQL sin configuración especial
# MAGIC 4. **Separación de responsabilidades**: Databricks procesa, Azure SQL sirve
# MAGIC 5. **Costos**: para dashboards de baja concurrencia, Azure SQL Basic ($5/mes) es
# MAGIC    más barato que mantener un SQL Warehouse de Databricks encendido
# MAGIC
# MAGIC ## Estrategia de sincronización
# MAGIC
# MAGIC - **Modo de escritura**: `overwrite` (idempotente, se puede re-ejecutar)
# MAGIC - **Frecuencia**: típicamente nightly batch (después de actualizar Gold)
# MAGIC - **Tablas sincronizadas**: 5 dimensiones + 1 fact + 2 KPIs agregados = 8 tablas
# MAGIC - **Multi-ambiente**: el widget `entorno` determina la BD destino (`ecv-dev` o `ecv-prod`)
# MAGIC
# MAGIC ## Notas técnicas importantes
# MAGIC
# MAGIC ### Driver JDBC
# MAGIC Usamos `com.microsoft.sqlserver.jdbc.SQLServerDriver` que viene preinstalado en
# MAGIC Databricks Runtime. **No requiere allowlist** (a diferencia de MySQL Connector/J).
# MAGIC
# MAGIC ### Tipos Decimal
# MAGIC Spark Decimal(14,2) se mapea automáticamente a SQL Server `decimal(14,2)`.
# MAGIC
# MAGIC ### Tipos Boolean
# MAGIC SQL Server no tiene tipo BOOLEAN nativo. JDBC los convierte a `BIT` (0/1).
# MAGIC Power BI los reconoce como booleanos sin problema.
# MAGIC
# MAGIC ### Particiones Spark
# MAGIC Las tablas Spark están particionadas por `anio_encuesta`. Al exportar, esa columna
# MAGIC queda como una columna normal en SQL Server. Si en SQL Server se quiere mejorar
# MAGIC performance, se puede crear un índice manualmente sobre esa columna.

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType
)
from datetime import datetime, timezone
import uuid


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────
# WIDGETS
# ─────────────────────────────────────────────────────────────────────────

dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod"], "Entorno")
dbutils.widgets.text("secret_scope", "akv-ecv", "Secret Scope")
dbutils.widgets.dropdown("modo_escritura", "overwrite", ["overwrite", "append"], "Modo escritura")

entorno         = dbutils.widgets.get("entorno")
secret_scope    = dbutils.widgets.get("secret_scope")
modo_escritura  = dbutils.widgets.get("modo_escritura")

catalog_name = f"ecv_{entorno}"
db_name_sql  = f"ecv-{entorno}"   # ecv-dev o ecv-prod (importante: guion, no underscore)
run_id       = str(uuid.uuid4())

print("=" * 65)
print(f"  Run ID          : {run_id}")
print(f"  Catálogo origen : {catalog_name}")
print(f"  BD destino SQL  : {db_name_sql}")
print(f"  Modo escritura  : {modo_escritura}")
print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer credenciales del Key Vault

# COMMAND ----------

try:
    sql_host = dbutils.secrets.get(scope=secret_scope, key="azuresql-host")
    sql_user = dbutils.secrets.get(scope=secret_scope, key="azuresql-user")
    sql_pwd  = dbutils.secrets.get(scope=secret_scope, key="azuresql-password")

    print(f"✅ Secretos cargados:")
    print(f"   Host: {sql_host}")
    print(f"   User: {sql_user}")
    print(f"   Pwd : ***{'X' * 3}")
except Exception as e:
    print(f"❌ Error leyendo secretos: {e}")
    raise

# Construir URL JDBC para Azure SQL Database
sql_url = (
    f"jdbc:sqlserver://{sql_host}:1433;"
    f"database={db_name_sql};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;"
    "loginTimeout=30;"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utilidades

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


def sincronizar_tabla(tabla_origen: str, tabla_destino_sql: str, batch_size: int = 5000) -> int:
    """
    Sincroniza una tabla Delta del catálogo Gold hacia Azure SQL Database.

    Parámetros:
    - tabla_origen: nombre completo en Databricks (ej: 'ecv_dev.gold.dim_persona')
    - tabla_destino_sql: nombre simple para SQL Server (ej: 'dim_persona')
    - batch_size: número de filas por batch en el INSERT (5000 es óptimo para Azure SQL Basic)

    Devuelve: número de registros escritos.
    """
    inicio = utc_now()

    try:
        df = spark.table(tabla_origen)
        registros = df.count()

        (df.write
            .format("jdbc")
            .option("url", sql_url)
            .option("dbtable", tabla_destino_sql)
            .option("user", sql_user)
            .option("password", sql_pwd)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("batchsize", batch_size)
            .option("truncate", "true")  # vacía la tabla en SQL Server antes de insertar (más rápido que DROP+CREATE)
            .mode(modo_escritura)
            .save())

        print(f"  ✅ {tabla_destino_sql:35s}: {registros:>10} registros")
        registrar_auditoria(
            "nb_05_serving", "serving", tabla_destino_sql,
            inicio, registros, registros, "OK"
        )
        return registros

    except Exception as e:
        msg = str(e)[:500]
        print(f"  ❌ {tabla_destino_sql:35s}: {msg}")
        registrar_auditoria(
            "nb_05_serving", "serving", tabla_destino_sql,
            inicio, 0, 0, "ERROR", msg
        )
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sincronización de las 8 tablas Gold
# MAGIC
# MAGIC Orden de sincronización:
# MAGIC
# MAGIC | # | Tabla origen (Databricks)            | Tabla destino (Azure SQL)        | Tipo            |
# MAGIC |---|--------------------------------------|----------------------------------|-----------------|
# MAGIC | 1 | gold.dim_tiempo                      | dim_tiempo                       | Dimensión       |
# MAGIC | 2 | gold.dim_ubicacion                   | dim_ubicacion                    | Dimensión       |
# MAGIC | 3 | gold.dim_persona                     | dim_persona                      | Dimensión       |
# MAGIC | 4 | gold.dim_vivienda                    | dim_vivienda                     | Dimensión       |
# MAGIC | 5 | gold.dim_educacion                   | dim_educacion                    | Dimensión       |
# MAGIC | 6 | gold.fac_ecv                         | fac_ecv                          | Tabla de hechos |
# MAGIC | 7 | gold.kpi_becas_anuales               | kpi_becas_anuales                | Agregado        |
# MAGIC | 8 | gold.kpi_condiciones_vida_region     | kpi_condiciones_vida_region      | Agregado        |
# MAGIC
# MAGIC

# COMMAND ----------

print(f"🚀 Iniciando sincronización Gold → Azure SQL ({db_name_sql})")
print("=" * 70)

tablas_a_sincronizar = [
    # (tabla_origen, tabla_destino_sql)
    ("dim_tiempo",                   "dim_tiempo"),
    ("dim_ubicacion",                "dim_ubicacion"),
    ("dim_persona",                  "dim_persona"),
    ("dim_vivienda",                 "dim_vivienda"),
    ("dim_educacion",                "dim_educacion"),
    ("fac_ecv",                      "fac_ecv"),
    ("kpi_becas_anuales",            "kpi_becas_anuales"),
    ("kpi_condiciones_vida_region",  "kpi_condiciones_vida_region"),
]

total_sincronizado = 0
errores = 0

for tabla_origen, tabla_destino in tablas_a_sincronizar:
    nombre_completo = f"{catalog_name}.gold.{tabla_origen}"
    try:
        registros = sincronizar_tabla(nombre_completo, tabla_destino)
        total_sincronizado += registros
    except Exception:
        errores += 1
        # Continuar con las siguientes tablas en lugar de detener todo

print("=" * 70)
print(f"\n📊 Resumen:")
print(f"   Tablas procesadas : {len(tablas_a_sincronizar)}")
print(f"   Errores           : {errores}")
print(f"   Total registros   : {total_sincronizado:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación: conteos en Azure SQL coinciden con Gold

# COMMAND ----------

print("=" * 70)
print("VERIFICACIÓN DE INTEGRIDAD: Gold (Databricks) vs Serving (Azure SQL)")
print("=" * 70)
print(f"{'Tabla':<35s}{'Gold':>12s}{'Azure SQL':>12s}{'Match':>8s}")
print("-" * 70)

todas_ok = True

for tabla_origen, tabla_destino in tablas_a_sincronizar:
    # Conteo en Databricks Gold
    count_gold = spark.table(f"{catalog_name}.gold.{tabla_origen}").count()

    # Conteo en Azure SQL (lectura de prueba)
    try:
        df_sql = (
            spark.read
            .format("jdbc")
            .option("url", sql_url)
            .option("query", f"SELECT COUNT(*) AS total FROM [{tabla_destino}]")
            .option("user", sql_user)
            .option("password", sql_pwd)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .load()
        )
        count_sql = df_sql.collect()[0]["total"]

        match = "✅ OK" if count_gold == count_sql else "❌ DIF"
        if count_gold != count_sql:
            todas_ok = False
        print(f"{tabla_destino:<35s}{count_gold:>12,}{count_sql:>12,}{match:>8s}")
    except Exception as e:
        print(f"{tabla_destino:<35s}{count_gold:>12,}{'ERROR':>12s}{'❌':>8s}")
        todas_ok = False

print("-" * 70)
if todas_ok:
    print("✅ TODAS LAS TABLAS COINCIDEN PERFECTAMENTE")
else:
    print("❌ HAY DIFERENCIAS - revisar logs anteriores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validación analítica desde Azure SQL
# MAGIC
# MAGIC Ejecuta los mismos KPIs que tu proyecto 2021, pero ahora desde el lado de Azure SQL
# MAGIC para confirmar que el serving layer está listo para Power BI.

# COMMAND ----------

print("=" * 70)
print("VALIDACIÓN ANALÍTICA: KPIs leyendo directo desde Azure SQL")
print("=" * 70)

# KPI 1: Margen Cobertura de Becas por año
print("\n📊 KPI 1: Margen de Cobertura de Becas por año")
df_kpi_becas = (
    spark.read
    .format("jdbc")
    .option("url", sql_url)
    .option("query", """
        SELECT
            anio_encuesta,
            total_estudiantes,
            total_becados,
            margen_cobertura_becas_pct,
            crecimiento_becas_pct
        FROM kpi_becas_anuales
    """)
    .option("user", sql_user)
    .option("password", sql_pwd)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
    .orderBy("anio_encuesta")   # orden en Spark: SQL Server no permite ORDER BY en subqueries sin TOP
)
df_kpi_becas.show(truncate=False)

# KPI 2: Top 5 regiones con más viviendas en riesgo
print("\n📊 KPI 2: Top 5 regiones con más viviendas en riesgo (2018)")
df_kpi_riesgo = (
    spark.read
    .format("jdbc")
    .option("url", sql_url)
    .option("query", """
        SELECT TOP 5
            region,
            total_personas,
            personas_en_vivienda_riesgo,
            pct_en_riesgo
        FROM kpi_condiciones_vida_region
        WHERE anio_encuesta = 2018
        ORDER BY pct_en_riesgo DESC
    """)
    .option("user", sql_user)
    .option("password", sql_pwd)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)
df_kpi_riesgo.show(truncate=False)

# KPI 3: Distribución por nivel educativo (consulta con JOIN)
print("\n📊 KPI 3: Distribución por nivel educativo (consulta con JOIN fact-dim)")
df_kpi_nivel = (
    spark.read
    .format("jdbc")
    .option("url", sql_url)
    .option("query", """
        SELECT TOP 10
            de.nivel_educativo,
            COUNT(*) AS total,
            SUM(CAST(f.total_becados AS INT)) AS becados,
            CAST(SUM(CAST(f.total_becados AS INT)) * 100.0 / COUNT(*) AS DECIMAL(5,2)) AS cobertura_pct
        FROM fac_ecv f
        INNER JOIN dim_educacion de ON f.sk_educacion = de.sk_educacion
        WHERE de.actualmente_estudia = 1
        GROUP BY de.nivel_educativo
        ORDER BY cobertura_pct DESC
    """)
    .option("user", sql_user)
    .option("password", sql_pwd)
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    .load()
)
df_kpi_nivel.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Resumen de la corrida

# COMMAND ----------

df_resumen = (
    spark.table(f"{catalog_name}.audit.pipeline_runs")
    .filter(F.col("run_id") == run_id)
    .select("tabla_destino", "estado", "registros_out", "duracion_seg")
    .orderBy("inicio_utc")
)
display(df_resumen)

# COMMAND ----------

dbutils.notebook.exit(f"SERVING_OK run_id={run_id} entorno={entorno}")
