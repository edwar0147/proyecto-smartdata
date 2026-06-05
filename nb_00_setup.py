# Databricks notebook source
# MAGIC %md
# MAGIC # NB 00 - Setup inicial del proyecto ECV
# MAGIC
# MAGIC **Proyecto:** Análisis de Encuesta de Calidad de Vida (ECV) - DANE Colombia
# MAGIC **Arquitectura:** Medallón (Bronze / Silver / Gold) sobre Azure + Databricks + Unity Catalog
# MAGIC **Autor:** Eduar Alonso Caro Montoya
# MAGIC
# MAGIC ## Propósito
# MAGIC Este notebook prepara el workspace de Databricks dejando todo listo para los notebooks de carga.
# MAGIC Se ejecuta **una sola vez por entorno** (dev / prod). Las cargas posteriores no lo necesitan.
# MAGIC
# MAGIC ## Lo que hace
# MAGIC 1. Lee parámetros de entorno vía widgets
# MAGIC 2. Valida acceso al Secret Scope respaldado por Azure Key Vault
# MAGIC 3. Crea (si no existen) el catálogo y los esquemas en Unity Catalog:
# MAGIC    - `bronze` — datos crudos
# MAGIC    - `silver` — datos limpios y normalizados
# MAGIC    - `gold` — modelo dimensional analítico
# MAGIC    - `ref` — tablas de referencia (DIVIPOLA)
# MAGIC    - `audit` — auditoría del pipeline
# MAGIC 4. Crea las External Locations apuntando a los contenedores del Storage Account
# MAGIC 5. Crea las tablas de auditoría y control
# MAGIC
# MAGIC ## Prerrequisitos
# MAGIC - Storage Account con ADLS Gen2 y contenedores: `raw`, `bronze`, `silver`, `gold`
# MAGIC - Access Connector for Databricks con rol "Storage Blob Data Contributor" sobre el Storage
# MAGIC - Storage Credential `sc_databricks_ecv` creada y validada en Unity Catalog
# MAGIC - Key Vault con Secret Scope `kv-ecv` configurado en Databricks
# MAGIC - Workspace Databricks Premium con Unity Catalog activo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parámetros de ejecución
# MAGIC
# MAGIC Los parámetros se inyectan vía widgets. Esto permite reutilizar el mismo notebook
# MAGIC para dev y prod sin modificar código.

# COMMAND ----------

dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod"], "Entorno")
dbutils.widgets.text("storage_account", "saencuestadev", "Storage Account")
dbutils.widgets.text("secret_scope", "akv-ecv", "Secret Scope")
dbutils.widgets.text("storage_credential", "sc_databricks_ecv", "Storage Credential")

entorno            = dbutils.widgets.get("entorno")
storage_account    = dbutils.widgets.get("storage_account")
secret_scope       = dbutils.widgets.get("secret_scope")
storage_credential = dbutils.widgets.get("storage_credential")

catalog_name = f"ecv_{entorno}"

print("=" * 60)
print(f"  Entorno            : {entorno}")
print(f"  Storage account    : {storage_account}")
print(f"  Secret scope       : {secret_scope}")
print(f"  Storage credential : {storage_credential}")
print(f"  Catálogo destino   : {catalog_name}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validación de Secret Scope
# MAGIC
# MAGIC Verifica que el Secret Scope respaldado por Azure Key Vault esté configurado y que
# MAGIC Databricks pueda leerlo. Si esto falla, hay que:
# MAGIC
# MAGIC 1. Asegurar que el scope existe (`#secrets/createScope` en la URL del workspace)
# MAGIC 2. Asegurar que el Service Principal de AzureDatabricks tiene el rol
# MAGIC    "Key Vault Secrets User" sobre el Key Vault
# MAGIC
# MAGIC Los secretos que el pipeline espera son:
# MAGIC - `mysql-host`, `mysql-user`, `mysql-password`, `mysql-database` (para MySQL DIVIPOLA)
# MAGIC - `azuresql-connection-string` (para serving layer, opcional)

# COMMAND ----------

try:
    scopes = [s.name for s in dbutils.secrets.listScopes()]
    if secret_scope not in scopes:
        raise Exception(
            f"El secret scope '{secret_scope}' no existe. "
            f"Scopes disponibles: {scopes}. "
            f"Crear con #secrets/createScope en la URL del workspace."
        )
    print(f"OK  Secret scope '{secret_scope}' encontrado.")

    # Listar (sin imprimir valores) los secretos disponibles
    secretos_disponibles = [s.key for s in dbutils.secrets.list(secret_scope)]
    print(f"OK  Secretos disponibles: {sorted(secretos_disponibles)}")

    # Avisar si faltan secretos esperados (no es error fatal, los notebooks consumidores deciden)
    secretos_esperados = [
        "mysql-host",
        "mysql-user",
        "mysql-password",
        "mysql-database",
        "azuresql-connection-string",
    ]
    faltantes = [s for s in secretos_esperados if s not in secretos_disponibles]
    if faltantes:
        print(f"AVISO: faltan secretos esperados: {faltantes}")
        print("       Los notebooks que los necesiten fallarán hasta que se creen.")
    else:
        print(f"OK  Todos los secretos esperados están configurados.")
except Exception as e:
    print(f"ERROR validando secret scope: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Creación del catálogo y esquemas en Unity Catalog
# MAGIC
# MAGIC Estructura objetivo:
# MAGIC
# MAGIC ```
# MAGIC catalog: ecv_dev   (o ecv_prod)
# MAGIC ├── schema: bronze   - datos crudos sin transformar
# MAGIC ├── schema: silver   - datos limpios, decodificados, deduplicados
# MAGIC ├── schema: gold     - modelo dimensional analítico
# MAGIC ├── schema: ref      - tablas de referencia (DIVIPOLA)
# MAGIC └── schema: audit    - auditoría y control del pipeline
# MAGIC ```

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"USE CATALOG {catalog_name}")
print(f"OK  Catálogo {catalog_name} listo")

esquemas = ["bronze", "silver", "gold", "ref", "audit"]
for esquema in esquemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{esquema}")
    print(f"OK  Esquema {catalog_name}.{esquema} listo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. External Locations en ADLS Gen2
# MAGIC
# MAGIC Las External Locations declaran ante Unity Catalog que ciertos contenedores
# MAGIC del Storage Account son accesibles vía el Storage Credential creado.
# MAGIC Esto permite a Databricks leer/escribir en esos contenedores con permisos
# MAGIC gestionados por Unity Catalog (no por SAS tokens ni access keys).

# COMMAND ----------

contenedores = ["raw", "bronze", "silver", "gold"]

for c in contenedores:
    url = f"abfss://{c}@{storage_account}.dfs.core.windows.net/"
    location_name = f"ext_loc_{c}"
    try:
        spark.sql(f"""
            CREATE EXTERNAL LOCATION IF NOT EXISTS {location_name}
            URL '{url}'
            WITH (STORAGE CREDENTIAL {storage_credential})
            COMMENT 'External location para capa {c} del proyecto ECV ({entorno})'
        """)
        print(f"OK  External location {location_name} -> {url}")
    except Exception as e:
        print(f"AVISO al crear {location_name}: {str(e)[:200]}")
        print(f"      (puede ser normal si ya existía con otra configuración)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tablas de auditoría y control
# MAGIC
# MAGIC ### `audit.pipeline_runs`
# MAGIC Registro fila a fila de cada ejecución de cada notebook con conteos y duración.
# MAGIC Permite responder: *"¿qué tablas se cargaron ayer y cuánto demoró cada una?"*
# MAGIC
# MAGIC ### `audit.carga_anual`
# MAGIC Resumen anual: qué años se han cargado, cuándo, con qué volumen.
# MAGIC Útil para saber rápidamente *"¿ya cargamos 2019?"*.

# COMMAND ----------

# Tabla de auditoría detallada - cada ejecución de cada tabla
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.audit.pipeline_runs (
        run_id           STRING,
        notebook         STRING,
        capa             STRING,
        tabla_destino    STRING,
        registros_in     BIGINT,
        registros_out    BIGINT,
        estado           STRING,
        mensaje          STRING,
        inicio_utc       TIMESTAMP,
        fin_utc          TIMESTAMP,
        duracion_seg     DOUBLE
    )
    USING DELTA
""")
print(f"OK  Tabla {catalog_name}.audit.pipeline_runs lista")

# Tabla de control anual - resumen por año cargado
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.audit.carga_anual (
        anio_encuesta    INT,
        tabla            STRING,
        registros        BIGINT,
        fecha_carga      TIMESTAMP,
        run_id           STRING
    )
    USING DELTA
""")
print(f"OK  Tabla {catalog_name}.audit.carga_anual lista")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificación final
# MAGIC
# MAGIC Lista todo lo creado para confirmar visualmente que el setup quedó correcto.

# COMMAND ----------

print("=" * 60)
print("RESUMEN DEL SETUP")
print("=" * 60)

print(f"\nCatálogo activo: {catalog_name}")

print("\nEsquemas:")
df_esquemas = spark.sql(f"SHOW SCHEMAS IN {catalog_name}")
df_esquemas.show(truncate=False)

print("Tablas en audit:")
df_audit = spark.sql(f"SHOW TABLES IN {catalog_name}.audit")
df_audit.show(truncate=False)

print("External Locations:")
df_ext = spark.sql("SHOW EXTERNAL LOCATIONS").filter("name LIKE 'ext_loc_%'")
df_ext.select("name", "url").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cierre

# COMMAND ----------

print("=" * 60)
print("SETUP COMPLETADO")
print("=" * 60)
print(f"Catálogo : {catalog_name}")
print(f"Esquemas : bronze, silver, gold, ref, audit")
print(f"Storage  : {storage_account} (raw, bronze, silver, gold)")
print(f"Secrets  : scope '{secret_scope}'")
print()
print("Próximos pasos:")
print("  1. Subir CSV ECV al contenedor raw bajo raw/ecv/<año>/")
print("  2. Subir Excel DIVIPOLA al contenedor raw bajo raw/divipola/")
print("  3. Ejecutar nb_01_bronze_ingesta")

#dbutils.notebook.exit("SETUP_OK")
