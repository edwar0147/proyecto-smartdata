# Databricks notebook source
dbutils.widgets.dropdown("entorno", "dev", ["dev", "prod"], "Entorno")
entorno = dbutils.widgets.get("entorno")
catalog_name = f"ecv_{entorno}"

print(f"Catálogo configurado para ejecución: {catalog_name}")

# Limpiar tablas Bronze que están en modo append
# (para evitar duplicación de los CSV ECV)
for t in ["caract_comp_hogar", "datos_vivienda", "educacion"]:
    # CORRECCIÓN: Usar catalog_name en lugar de entorno
    spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.bronze.{t}")
    print(f"Borrada: {catalog_name}.bronze.{t}")

# Las DIVIPOLA y MySQL usan overwrite, no necesitan borrarse
print("\nListo, ahora puedes ejecutar nb_01")

# COMMAND ----------

from pyspark.sql import functions as F

print("=" * 70)
print("VALIDACIÓN: ref.divipola consolidado")
print("=" * 70)

# CORRECCIÓN: Usar catalog_name dinámico, no ecv_dev quemado
df_dvp = spark.table(f"{catalog_name}.ref.divipola")
total = df_dvp.count()

print(f"\nTotal municipios consolidados: {total}")
print(f"Esperado: ~1.122 (todos los municipios de Colombia)\n")

print("Distribución por origen:")
df_dvp.groupBy("origen_divipola").count().orderBy(F.desc("count")).show()

print("Top 10 departamentos con más municipios:")
df_dvp.groupBy("nombre_departamento").count() \
    .orderBy(F.desc("count")).show(10, truncate=False)

print("Departamentos con NULL (debería ser 0):")
df_dvp.filter(F.col("nombre_departamento").isNull()) \
    .groupBy("cod_departamento").count().show()
