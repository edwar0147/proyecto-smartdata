# Databricks notebook source
# MAGIC %md
# MAGIC # NB 01 - Capa Bronze: Ingesta de fuentes
# MAGIC
# MAGIC **Proyecto:** Análisis de Encuesta de Calidad de Vida (ECV) - DANE Colombia
# MAGIC **Capa:** Bronze (Extract)
# MAGIC **Autor:** Eduar Alonso Caro Montoya
# MAGIC
# MAGIC ## Objetivo
# MAGIC Ingerir los datos crudos desde las fuentes externas a la capa Bronze como tablas Delta,
# MAGIC conservando el dato tal como llega y agregando metadatos de control.
# MAGIC
# MAGIC ## Fuentes
# MAGIC
# MAGIC | Fuente               | Origen                          | Tablas destino Bronze              |
# MAGIC |----------------------|---------------------------------|------------------------------------|
# MAGIC | CSV ECV (multi-año)  | Azure Blob Storage `raw/ecv/`   | caract_comp_hogar, datos_vivienda, educacion |
# MAGIC | Excel DIVIPOLA       | Azure Blob Storage `raw/divipola/` | departamentos, municipios_excel |
# MAGIC | MySQL DIVIPOLA       | Azure Database for MySQL        | municipios_mysql                   |
# MAGIC
# MAGIC ## Estrategia de carga
# MAGIC - **CSV ECV**: append-only por año (cada ejecución agrega nuevos años sin tocar los previos)
# MAGIC - **Excel DIVIPOLA**: overwrite (catálogo de referencia, no histórico)
# MAGIC - **MySQL DIVIPOLA**: overwrite (catálogo de referencia)
# MAGIC
# MAGIC ## Reglas técnicas
# MAGIC - **NO** se usa Spark SQL (solo DataFrame API en PySpark) para transformaciones
# MAGIC - Particionado por `anio_encuesta` en las tablas CSV
# MAGIC - Columnas de control obligatorias: `fecha_ingesta`, `origen`, `archivo_fuente`, `anio_encuesta`, `run_id`
# MAGIC - Manejo tolerante a archivos faltantes (no rompe si un año no existe en el storage)
# MAGIC - MySQL es opcional: si no hay credenciales configuradas, se omite sin error
# MAGIC
# MAGIC ## Formato detectado en los CSV del DANE
# MAGIC - Delimitador: `;` (punto y coma)
# MAGIC - Encoding: UTF-8 con BOM (Spark lo ignora automáticamente)
# MAGIC - Final de línea: CRLF (Windows)
# MAGIC - Nulos: representados como espacios en blanco → se convierten a NULL al cargar

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros y librerías

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, TimestampType
)
from datetime import datetime, timezone


def utc_now() -> datetime:
    """Reemplazo de utc_now() (deprecada en Python 3.12+).
    Devuelve datetime tz-aware en UTC."""
    return datetime.now(timezone.utc)
import uuid

# Widgets
dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod"], "Entorno")
dbutils.widgets.text("storage_account", "saencuestadev", "Storage Account")
dbutils.widgets.text("secret_scope", "akv-ecv", "Secret Scope")
dbutils.widgets.multiselect("anios", "2017",
    ["2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025"],
    "Años a cargar")
dbutils.widgets.dropdown("cargar_divipola", "si", ["si", "no"], "¿Cargar DIVIPOLA?")
dbutils.widgets.dropdown("cargar_mysql", "no", ["si", "no"], "¿Cargar MySQL?")

entorno          = dbutils.widgets.get("entorno")
storage_account  = dbutils.widgets.get("storage_account")
secret_scope     = dbutils.widgets.get("secret_scope")
anios            = [int(a) for a in dbutils.widgets.get("anios").split(",")]
cargar_divipola  = dbutils.widgets.get("cargar_divipola") == "si"
cargar_mysql     = dbutils.widgets.get("cargar_mysql") == "si"
catalog_name     = f"ecv_{entorno}"
run_id           = str(uuid.uuid4())

print("=" * 60)
print(f"  Run ID            : {run_id}")
print(f"  Catálogo          : {catalog_name}")
print(f"  Años a procesar   : {anios}")
print(f"  Cargar DIVIPOLA   : {cargar_divipola}")
print(f"  Cargar MySQL      : {cargar_mysql}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Utilidades comunes

# COMMAND ----------

def registrar_auditoria(
    notebook: str, capa: str, tabla: str, inicio: datetime,
    registros_in: int, registros_out: int, estado: str, mensaje: str = "",
):
    """Inserta una fila en audit.pipeline_runs."""
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


def registrar_carga_anual(tabla: str, anio: int, registros: int):
    """Registra resumen anual de carga (sobrescribe si ya existía esa tabla+año)."""
    fila_df = spark.createDataFrame(
        [(anio, tabla, registros, utc_now(), run_id)],
        schema=StructType([
            StructField("anio_encuesta", IntegerType()),
            StructField("tabla", StringType()),
            StructField("registros", LongType()),
            StructField("fecha_carga", TimestampType()),
            StructField("run_id", StringType()),
        ])
    )
    # Merge: si ya existe ese año + tabla, actualiza; si no, inserta
    from delta.tables import DeltaTable
    tabla_audit = f"{catalog_name}.audit.carga_anual"
    if spark.catalog.tableExists(tabla_audit):
        DeltaTable.forName(spark, tabla_audit).alias("t") \
            .merge(
                fila_df.alias("s"),
                "t.tabla = s.tabla AND t.anio_encuesta = s.anio_encuesta"
            ) \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        fila_df.write.format("delta").mode("append").saveAsTable(tabla_audit)


def agregar_columnas_control(df: DataFrame, origen: str, archivo: str, anio: int) -> DataFrame:
    """Agrega columnas técnicas a cualquier DataFrame antes de escribirlo en Bronze."""
    return (
        df.withColumn("fecha_ingesta", F.current_timestamp())
          .withColumn("origen", F.lit(origen))
          .withColumn("archivo_fuente", F.lit(archivo))
          .withColumn("anio_encuesta", F.lit(anio).cast(IntegerType()))
          .withColumn("run_id", F.lit(run_id))
    )


def limpiar_espacios_a_null(df: DataFrame) -> DataFrame:
    """Convierte cadenas vacías o solo espacios a NULL en todas las columnas string."""
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for c in string_cols:
        df = df.withColumn(
            c,
            F.when(F.trim(F.col(c)) == "", F.lit(None).cast(StringType()))
             .otherwise(F.col(c))
        )
    return df


def normalizar_nombres_columnas(df: DataFrame) -> DataFrame:
    """Normaliza nombres de columnas para que sean compatibles con Delta:
    - Quita tildes y eñes
    - Reemplaza espacios y caracteres especiales por '_'
    - Pasa todo a minúsculas
    - Colapsa múltiples '_' consecutivos
    Necesario para columnas que vienen de Excel o MySQL con nombres como
    'Código Departamento' o 'CódigoDepartamento'.
    """
    import unicodedata
    import re

    def normalizar(nombre: str) -> str:
        # Remover tildes y caracteres no-ASCII (decomposición Unicode)
        sin_tildes = unicodedata.normalize("NFKD", nombre).encode("ASCII", "ignore").decode("ASCII")
        # A minúsculas
        sin_tildes = sin_tildes.lower()
        # Reemplazar cualquier carácter no alfanumérico por '_'
        normalizado = re.sub(r"[^a-z0-9_]+", "_", sin_tildes)
        # Colapsar múltiples '_' y eliminar bordes
        normalizado = re.sub(r"_+", "_", normalizado).strip("_")
        return normalizado

    for c in df.columns:
        nuevo = normalizar(c)
        if nuevo != c:
            df = df.withColumnRenamed(c, nuevo)
    return df


def ruta_raw(subpath: str) -> str:
    """Construye una ruta abfss para el contenedor raw."""
    return f"abfss://raw@{storage_account}.dfs.core.windows.net/{subpath}"


def archivo_existe(path: str) -> bool:
    """Verifica si un archivo existe en ADLS sin fallar."""
    try:
        dbutils.fs.ls(path)
        return True
    except Exception:
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Lectura de CSV del DANE
# MAGIC
# MAGIC Convenciones detectadas y validadas con archivos 2017:
# MAGIC - Delimitador `;`
# MAGIC - UTF-8 con BOM (Spark lo ignora)
# MAGIC - Nulos como espacios en blanco
# MAGIC - En Bronze TODO va como string. El tipado se hace en Silver.

# COMMAND ----------

def leer_csv_dane(path: str) -> DataFrame:
    """Lee un CSV del DANE con las convenciones reales."""
    return (
        spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("encoding", "UTF-8")
        .option("inferSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("multiLine", "false")
        .csv(path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mapeo de archivos por año
# MAGIC
# MAGIC Diccionario explícito que mapea cada tabla destino a su archivo físico por año.
# MAGIC **Para agregar un año nuevo**: descomentar (o agregar) la línea correspondiente,
# MAGIC verificar el nombre real del archivo en el storage, y volver a ejecutar.

# COMMAND ----------

ARCHIVOS_ECV = {
    "caract_comp_hogar": {
        2017: "Caracteristicas_y_composicion_del_hogar2017.csv",
        2018: "Caracteristicas_y_composicion_del_hogar2018.csv",
        # 2019: "Caracteristicas_y_composicion_del_hogar2019.csv",
        # 2020: "Caracteristicas_y_composicion_del_hogar2020.csv",
        # 2021: "Caracteristicas_y_composicion_del_hogar2021.csv",
        # 2022: "Caracteristicas_y_composicion_del_hogar2022.csv",
        # 2023: "Caracteristicas_y_composicion_del_hogar2023.csv",
        # 2024: "Caracteristicas_y_composicion_del_hogar2024.csv",
        # 2025: "Caracteristicas_y_composicion_del_hogar2025.csv",
    },
    "datos_vivienda": {
        2017: "Datos_de_la_vivienda_Actualizada2017.csv",
        2018: "Datos_de_la_vivienda_Actualizada2018.csv",
        # 2019: "Datos_de_la_vivienda_Actualizada2019.csv",
    },
    "educacion": {
        2017: "Educación2017.csv",
        2018: "Educación2018.csv",
        # 2019: "Educación2019.csv",
    },
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ingesta de los CSV ECV

# COMMAND ----------

for nombre_tabla, archivos_por_anio in ARCHIVOS_ECV.items():
    for anio in anios:
        inicio = utc_now()

        if anio not in archivos_por_anio:
            print(f"SKIP {nombre_tabla} [{anio}]: no hay archivo definido en el diccionario")
            continue

        nombre_archivo = archivos_por_anio[anio]
        path_csv = ruta_raw(f"ecv/{anio}/{nombre_archivo}")
        tabla_destino = f"{catalog_name}.bronze.{nombre_tabla}"

        # Verificación previa: el archivo existe en el storage
        if not archivo_existe(path_csv):
            mensaje = f"Archivo no encontrado: {path_csv}"
            print(f"SKIP {nombre_tabla} [{anio}]: {mensaje}")
            registrar_auditoria(
                "nb_01_bronze_ingesta", "bronze", nombre_tabla,
                inicio, 0, 0, "OMITIDO", mensaje
            )
            continue

        try:
            df = leer_csv_dane(path_csv)
            df = limpiar_espacios_a_null(df)
            registros = df.count()

            df_final = agregar_columnas_control(
                df, origen="blob_csv", archivo=nombre_archivo, anio=anio
            )

            (df_final.write
                .format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("anio_encuesta")
                .saveAsTable(tabla_destino))

            print(f"OK   {tabla_destino} [{anio}] <- {nombre_archivo} ({registros} registros)")
            registrar_auditoria(
                "nb_01_bronze_ingesta", "bronze", nombre_tabla,
                inicio, registros, registros, "OK"
            )
            registrar_carga_anual(nombre_tabla, anio, registros)

        except Exception as e:
            mensaje = str(e)[:500]
            print(f"ERR  {tabla_destino} [{anio}]: {mensaje}")
            registrar_auditoria(
                "nb_01_bronze_ingesta", "bronze", nombre_tabla,
                inicio, 0, 0, "ERROR", mensaje
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Ingesta del Excel DIVIPOLA
# MAGIC
# MAGIC Carga 2 hojas del Excel `Departamentos_y_Municipios.xls`:
# MAGIC - **Hoja "Departamentos"**: maestro de los 33 departamentos de Colombia
# MAGIC - **Hoja "Municipios (3)"**: 499 municipios principales
# MAGIC
# MAGIC El resto de municipios (~623) vendrán desde MySQL en la siguiente sección.
# MAGIC
# MAGIC ### Prerrequisitos
# MAGIC El cluster debe tener instalada la librería `com.crealytics:spark-excel_2.12:0.20.4`
# MAGIC (o versión compatible). Para instalarla:
# MAGIC 1. Compute → tu cluster → Libraries → Install new
# MAGIC 2. Library Source: Maven
# MAGIC 3. Coordinates: `com.crealytics:spark-excel_2.12:0.20.4`
# MAGIC ### si falla ingresar a esta ruta:
# MAGIC en Allowed JARs/Init Scripts agregar la libreria con la coordenada
# MAGIC https://adb-7405615669497279.19.azuredatabricks.net/governance/metastore?o=7405615669497279
# MAGIC
# MAGIC ### Si la librería no está instalada
# MAGIC Esta sección fallará y se registrará en auditoría, pero el resto del notebook continúa.

# COMMAND ----------

if cargar_divipola:
    excel_path = ruta_raw("divipola/Departamentos_y_Municipios.xls")

    if not archivo_existe(excel_path):
        print(f"AVISO: Excel DIVIPOLA no encontrado en {excel_path}")
        print(f"       Subir el archivo y volver a ejecutar esta celda.")
    else:
        # --- 6.1 Hoja Departamentos ---
        inicio = utc_now()
        tabla_destino = f"{catalog_name}.bronze.divipola_departamentos"

        try:
            df_dept = (
                spark.read
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("dataAddress", "'Departamentos'!A1")
                .load(excel_path)
            )
            df_dept = normalizar_nombres_columnas(df_dept)
            df_dept = limpiar_espacios_a_null(df_dept)
            registros = df_dept.count()

            df_final = agregar_columnas_control(
                df_dept, origen="excel", archivo="Departamentos_y_Municipios.xls#Departamentos", anio=0
            )

            (df_final.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(tabla_destino))

            print(f"OK   {tabla_destino} <- hoja 'Departamentos' ({registros} registros)")
            registrar_auditoria(
                "nb_01_bronze_ingesta", "bronze", "divipola_departamentos",
                inicio, registros, registros, "OK"
            )
        except Exception as e:
            mensaje = str(e)[:500]
            print(f"ERR  {tabla_destino}: {mensaje}")
            registrar_auditoria(
                "nb_01_bronze_ingesta", "bronze", "divipola_departamentos",
                inicio, 0, 0, "ERROR", mensaje
            )

        # --- 6.2 Hoja Municipios (3) ---
        inicio = utc_now()
        tabla_destino = f"{catalog_name}.bronze.divipola_municipios_excel"

        try:
            df_mun = (
                spark.read
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("inferSchema", "false")
                .option("dataAddress", "'Municipios (3)'!A1")
                .load(excel_path)
            )
            df_mun = normalizar_nombres_columnas(df_mun)
            df_mun = limpiar_espacios_a_null(df_mun)
            registros = df_mun.count()

            df_final = agregar_columnas_control(
                df_mun, origen="excel", archivo="Departamentos_y_Municipios.xls#Municipios (3)", anio=0
            )

            (df_final.write
                .format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .saveAsTable(tabla_destino))

            print(f"OK   {tabla_destino} <- hoja 'Municipios (3)' ({registros} registros)")
            registrar_auditoria(
                "nb_01_bronze_ingesta", "bronze", "divipola_municipios_excel",
                inicio, registros, registros, "OK"
            )
        except Exception as e:
            mensaje = str(e)[:500]
            print(f"ERR  {tabla_destino}: {mensaje}")
            registrar_auditoria(
                "nb_01_bronze_ingesta", "bronze", "divipola_municipios_excel",
                inicio, 0, 0, "ERROR", mensaje
            )
else:
    print("SKIP DIVIPOLA Excel (cargar_divipola=no)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Ingesta de MySQL (municipios complementarios)
# MAGIC
# MAGIC Carga la tabla `municipios` de Azure Database for MySQL, que complementa
# MAGIC los municipios del Excel con los ~623 restantes para llegar a los ~1.122 de Colombia.
# MAGIC
# MAGIC ### Estructura esperada en MySQL
# MAGIC ```sql
# MAGIC CREATE TABLE municipios (
# MAGIC     CódigoDepartamento INT NOT NULL,
# MAGIC     NombreMunicipio VARCHAR(80) NOT NULL,
# MAGIC     Municipio_ID INT NOT NULL
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Prerrequisitos
# MAGIC Los secretos `mysql-host`, `mysql-user`, `mysql-password`, `mysql-database`
# MAGIC deben estar configurados en el Key Vault `kv-ecv`.
# MAGIC
# MAGIC Si no están, esta sección se omite (no rompe el notebook).

# COMMAND ----------

if cargar_mysql:
    inicio = utc_now()
    tabla_destino = f"{catalog_name}.bronze.divipola_municipios_mysql"

    try:
        # Leer credenciales desde Key Vault
        mysql_host     = dbutils.secrets.get(scope=secret_scope, key="mysql-host")
        mysql_user     = dbutils.secrets.get(scope=secret_scope, key="mysql-user")
        mysql_password = dbutils.secrets.get(scope=secret_scope, key="mysql-password")
        mysql_database = dbutils.secrets.get(scope=secret_scope, key="mysql-database")

        # Validar que no sean placeholders
        if any(v in ("", "placeholder") for v in [mysql_host, mysql_user, mysql_password, mysql_database]):
            raise Exception(
                "Credenciales MySQL son placeholder. "
                "Configurar valores reales en el Key Vault antes de ejecutar."
            )

        mysql_url = (
            f"jdbc:mysql://{mysql_host}:3306/{mysql_database}"
            "?useSSL=true&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        )

        df_muni = (
            spark.read
            .format("jdbc")
            .option("url", mysql_url)
            .option("dbtable", "municipios")
            .option("user", mysql_user)
            .option("password", mysql_password)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
        )

        # Normalizar nombres de columnas (MySQL usa 'CódigoDepartamento', etc.)
        df_muni = normalizar_nombres_columnas(df_muni)

        # Forzar todo a string para consistencia con el resto de Bronze
        for c in df_muni.columns:
            df_muni = df_muni.withColumn(c, F.col(c).cast(StringType()))

        registros = df_muni.count()

        df_final = agregar_columnas_control(
            df_muni, origen="mysql", archivo="municipios", anio=0
        )

        (df_final.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(tabla_destino))

        print(f"OK   {tabla_destino} <- MySQL.municipios ({registros} registros)")
        registrar_auditoria(
            "nb_01_bronze_ingesta", "bronze", "divipola_municipios_mysql",
            inicio, registros, registros, "OK"
        )
    except Exception as e:
        mensaje = str(e)[:500]
        print(f"ERR  {tabla_destino}: {mensaje}")
        registrar_auditoria(
            "nb_01_bronze_ingesta", "bronze", "divipola_municipios_mysql",
            inicio, 0, 0, "ERROR", mensaje
        )
else:
    print("SKIP MySQL (cargar_mysql=no)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumen de la corrida

# COMMAND ----------

df_resumen = (
    spark.table(f"{catalog_name}.audit.pipeline_runs")
    .filter(F.col("run_id") == run_id)
    .select("tabla_destino", "estado", "registros_out", "duracion_seg", "mensaje")
    .orderBy("inicio_utc")
)
display(df_resumen)

# Métricas del resumen
total       = df_resumen.count()
ok          = df_resumen.filter(F.col("estado") == "OK").count()
errores     = df_resumen.filter(F.col("estado") == "ERROR").count()
omitidos    = df_resumen.filter(F.col("estado") == "OMITIDO").count()

print(f"\nTotal operaciones: {total}")
print(f"  OK       : {ok}")
print(f"  ERROR    : {errores}")
print(f"  OMITIDO  : {omitidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Resumen anual de cargas (vista de control)

# COMMAND ----------

df_anual = (
    spark.table(f"{catalog_name}.audit.carga_anual")
    .orderBy("tabla", "anio_encuesta")
)
display(df_anual)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verificación rápida de los datos cargados

# COMMAND ----------

tablas_a_verificar = [
    "caract_comp_hogar",
    "datos_vivienda",
    "educacion",
]

if cargar_divipola:
    tablas_a_verificar += ["divipola_departamentos", "divipola_municipios_excel"]
if cargar_mysql:
    tablas_a_verificar += ["divipola_municipios_mysql"]

for t in tablas_a_verificar:
    nombre = f"{catalog_name}.bronze.{t}"
    if spark.catalog.tableExists(nombre):
        cnt = spark.table(nombre).count()
        print(f"  {nombre:60s} : {cnt:>10} registros")
    else:
        print(f"  {nombre:60s} : (no existe)")

# COMMAND ----------

exit_msg = f"BRONZE_OK ok={ok} errores={errores} omitidos={omitidos} run_id={run_id}"
print(exit_msg)
dbutils.notebook.exit(exit_msg)
