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
