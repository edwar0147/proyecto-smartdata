# Databricks notebook source
# MAGIC %md
# MAGIC # NB 02 - Capa Silver: Limpieza, decodificación y conformado
# MAGIC
# MAGIC **Proyecto:** Análisis de Encuesta de Calidad de Vida (ECV) - DANE Colombia
# MAGIC **Capa:** Silver (Transform)
# MAGIC **Autor:** Eduar Alonso Caro Montoya
# MAGIC
# MAGIC ## Validación oficial
# MAGIC Todos los catálogos de decodificación están validados contra el diccionario oficial del DANE
# MAGIC publicado en el portal de microdatos: https://microdatos.dane.gov.co/index.php/catalog/544
# MAGIC
# MAGIC ## Tablas Silver producidas
# MAGIC
# MAGIC | Tabla              | Origen Bronze                                    | Grano                            |
# MAGIC |--------------------|--------------------------------------------------|----------------------------------|
# MAGIC | silver.divipola    | divipola_departamentos + divipola_municipios_*   | 1 fila por municipio             |
# MAGIC | silver.hogar       | caract_comp_hogar                                | 1 fila por miembro del hogar     |
# MAGIC | silver.vivienda    | datos_vivienda                                   | 1 fila por vivienda              |
# MAGIC | silver.educacion   | educacion                                        | 1 fila por persona × año         |
# MAGIC | silver.persona     | hogar + educacion + divipola                     | Persona consolidada con migración |
# MAGIC
# MAGIC ## Reglas técnicas
# MAGIC - Solo PySpark DataFrame API (sin Spark SQL en transformaciones)
# MAGIC - Estrategia de escritura: `overwrite` (idempotente, se puede re-ejecutar sin duplicar)
# MAGIC - Particionado por `anio_encuesta` en las tablas de hechos
# MAGIC - REGION se incluye condicionalmente (NULL para 2017, valor real para 2018+)
# MAGIC - Lugar de nacimiento (P756S1/S2) se decodifica vía DIVIPOLA si está disponible

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

def registrar_auditoria(
    notebook: str, capa: str, tabla: str, inicio: datetime,
    registros_in: int, registros_out: int, estado: str, mensaje: str = "",
):
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


def aplicar_diccionario(df: DataFrame, columna_origen: str, columna_destino: str,
                        diccionario: dict) -> DataFrame:
    """Decodifica una columna usando un mapeo Python."""
    if columna_origen not in df.columns:
        # Si la columna no existe (ej. REGION en 2017), crear destino en NULL
        return df.withColumn(columna_destino, F.lit(None).cast(StringType()))
    mapping_expr = F.create_map(*[F.lit(x) for kv in diccionario.items() for x in kv])
    return df.withColumn(columna_destino, mapping_expr[F.col(columna_origen).cast("string")])


def a_booleano_si_no(df: DataFrame, columna: str, nueva: str) -> DataFrame:
    """Convierte códigos 1=Sí / 2=No del DANE a booleano nativo."""
    if columna not in df.columns:
        return df.withColumn(nueva, F.lit(None).cast(BooleanType()))
    return df.withColumn(
        nueva,
        F.when(F.col(columna) == "1", F.lit(True))
         .when(F.col(columna) == "2", F.lit(False))
         .otherwise(F.lit(None).cast(BooleanType()))
    )


def decimal_con_coma(col: str):
    """Convierte cadena con coma decimal a Double ('2072,536' → 2072.536)."""
    return F.regexp_replace(F.col(col), ",", ".").cast(DoubleType())


def valor_monetario_con_coma(col: str):
    """Convierte valores monetarios a Decimal(14,2)."""
    return F.regexp_replace(F.col(col), ",", ".").cast(DecimalType(14, 2))


def col_existe(df: DataFrame, columna: str) -> bool:
    """Devuelve True si la columna existe en el DataFrame (case-sensitive)."""
    return columna in df.columns


def renombrar_si_existe(df: DataFrame, rename_map: dict) -> DataFrame:
    """Renombra columnas según diccionario; ignora las que no existan."""
    for orig, dest in rename_map.items():
        if orig in df.columns:
            df = df.withColumnRenamed(orig, dest)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Catálogos VALIDADOS contra diccionario oficial DANE

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════════════
# MÓDULO VIVIENDA
# ═══════════════════════════════════════════════════════════════════════

# P1070 — Tipo de vivienda (V3792)
TIPO_VIVIENDA = {
    "1": "Casa",
    "2": "Apartamento",
    "3": "Cuarto(s)",
    "4": "Vivienda (casa) indígena",
    "5": "Otro tipo de vivienda (carpa, vagón, embarcación, cueva, etc.)",
}

# P4005 — Material predominante de paredes exteriores (V3793)
MATERIAL_PAREDES = {
    "1": "Bloque, ladrillo, piedra, madera pulida",
    "2": "Tapia pisada, adobe",
    "3": "Bahareque revocado",
    "4": "Bahareque sin revocar",
    "5": "Madera burda, tabla, tablón",
    "6": "Material prefabricado",
    "7": "Guadua, caña, esterilla, otro vegetal",
    "8": "Zinc, tela, carbón, latas, desechos, plástico",
    "9": "Sin paredes",
}

# P4015 — Material predominante de pisos (V3794)
MATERIAL_PISOS = {
    "1": "Alfombra o tapete de pared a pared",
    "2": "Madera pulida y lacada, parqué",
    "3": "Mármol",
    "4": "Baldosa, vinilo, tableta, ladrillo",
    "5": "Madera burda, tabla, tablón, otro vegetal",
    "6": "Cemento, gravilla",
    "7": "Tierra, arena",
}

# P4567 — Material predominante del techo (V3795)
MATERIAL_TECHO = {
    "1": "Plancha de concreto, cemento u hormigón",
    "2": "Tejas de barro",
    "3": "Teja de asbesto - cemento",
    "4": "Teja metálica o lámina de zinc",
    "5": "Teja plástica",
    "6": "Paja, palma u otros vegetales",
    "7": "Material de desecho (tela, cartón, latas, plástico, otros)",
}

# CLASE — Cabecera vs rural disperso (estándar DANE)
CLASE_GEOGRAFICA = {
    "1": "Cabecera (Urbano)",
    "2": "Centro poblado y rural disperso",
}

# REGION — Las 9 regiones del DANE (disponible desde ECV 2018)
REGION = {
    "1": "Caribe",
    "2": "Oriental",
    "3": "Central",
    "4": "Pacífica (sin valle)",
    "5": "Bogotá",
    "6": "Antioquia",
    "7": "Valle del Cauca",
    "8": "San Andrés",
    "9": "Orinoquía-Amazonía",
}

# ═══════════════════════════════════════════════════════════════════════
# MÓDULO HOGAR
# ═══════════════════════════════════════════════════════════════════════

SEXO = {"1": "Hombre", "2": "Mujer"}

# P5502 — Estado civil (códigos oficiales DANE)
ESTADO_CIVIL = {
    "1": "No casado(a), vive en pareja menos de 2 años",
    "2": "No casado(a), vive en pareja 2 años o más",
    "3": "Viudo(a)",
    "4": "Separado(a) o divorciado(a)",
    "5": "Soltero(a)",
    "6": "Casado(a)",
}

# P6080 — Pertenencia étnica
RAZA = {
    "1": "Indígena",
    "2": "Gitano (a) (Rom)",
    "3": "Raizal del archipiélago",
    "4": "Palenquero (a)",
    "5": "Negro (a), mulato (a) (afrodescendiente)",
    "6": "Ninguno de los anteriores",
}

# P756 — ¿Dónde nació?
LUGAR_NACIMIENTO = {
    "1": "En este municipio",
    "2": "En otro municipio colombiano",
    "3": "En otro país",
}

# ═══════════════════════════════════════════════════════════════════════
# MÓDULO EDUCACIÓN
# ═══════════════════════════════════════════════════════════════════════

# P8587 — Nivel educativo más alto alcanzado (V4246)
NIVEL_EDUCATIVO = {
    "1":  "Ninguno",
    "2":  "Preescolar",
    "3":  "Básica primaria (1°–5°)",
    "4":  "Básica secundaria (6°–9°)",
    "5":  "Media (10°–13°)",
    "6":  "Técnico sin título",
    "7":  "Técnico con título",
    "8":  "Tecnológico sin título",
    "9":  "Tecnológico con título",
    "10": "Universitario sin título",
    "11": "Universitario con título",
    "12": "Postgrado sin título",
    "13": "Postgrado con título",
}

# P6218 — Razón principal por la que NO estudia (V4245)
RAZON_NO_ESTUDIAR = {
    "1":  "Considera que no está en edad escolar",
    "2":  "Considera que ya terminó",
    "3":  "Falta de dinero o costos educativos elevados",
    "4":  "Debe encargarse de los oficios del hogar",
    "5":  "Por embarazo",
    "6":  "Por inseguridad o malos tratos en el establecimiento educativo",
    "7":  "Falta de cupos",
    "8":  "No existe centro educativo cercano",
    "9":  "Necesita trabajar",
    "10": "No le gusta o no le interesa el estudio",
    "11": "Por enfermedad",
    "12": "Necesita educación especial",
    "13": "Tuvieron que abandonar el lugar de residencia habitual",
    "14": "Sus padres o cuidador no lo considera importante",
    "15": "Por situaciones académicas (bajos resultados, repetición)",
    "16": "Otra razón",
}

# P5673 — Tipo de establecimiento (V4254)
TIPO_ESTABLECIMIENTO = {
    "1": "Oficial",
    "2": "No oficial",
}

# P1101 — Jornada educativa (V4257)
JORNADA_EDUCATIVA = {
    "1": "Mañana",
    "2": "Tarde",
    "3": "Noche",
    "4": "Única o completa",
    "5": "Fin de semana",
}

# P6229 — De quién recibió la beca (V4270)
ENTIDAD_BECA = {
    "1": "De la misma institución educativa",
    "2": "Icetex",
    "3": "Gobierno nacional o departamental",
    "4": "Gobierno distrital o municipal",
    "5": "Otra entidad pública",
    "6": "Empresa pública donde trabaja un familiar",
    "7": "Empresa privada donde trabaja un familiar",
    "8": "Otra entidad privada",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: DIVIPOLA (catálogo geográfico consolidado)
# MAGIC
# MAGIC Consolida en una sola tabla los municipios provenientes de Excel + MySQL,
# MAGIC junto con el maestro de departamentos. El resultado vive en el esquema `ref`
# MAGIC para indicar que es tabla de referencia (no se usa como dimensión analítica directa).

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.ref.divipola"

try:
    # ── Departamentos (Excel) ───────────────────────────────────────────
    df_dept = spark.table(f"{catalog_name}.bronze.divipola_departamentos")
    # Renombrar a snake_case y corregir typo "deapartamento" → "departamento"
    df_dept = renombrar_si_existe(df_dept, {
        "codigo_deapartamento": "cod_departamento",
        "codigo_departamento":  "cod_departamento",
        "nombre_departamento":  "nombre_departamento",
        "codigopais":           "cod_pais",
    })
    df_dept = df_dept.select(
        F.col("cod_departamento").cast(IntegerType()).alias("cod_departamento"),
        F.col("nombre_departamento").alias("nombre_departamento"),
    ).filter(F.col("cod_departamento").isNotNull()).dropDuplicates(["cod_departamento"])

    print(f"Departamentos cargados: {df_dept.count()}")

    # ── Municipios desde Excel ──────────────────────────────────────────
    df_mun_excel = spark.table(f"{catalog_name}.bronze.divipola_municipios_excel")
    df_mun_excel = renombrar_si_existe(df_mun_excel, {
        "codigo_departamento": "cod_departamento",
        "nombre_departamento": "nombre_departamento_excel",
        "codigo_municipio":    "cod_municipio_corto",
        "nombre_municipio":    "nombre_municipio",
        "municipio_id":        "cod_municipio",
    })

    df_mun_excel = df_mun_excel.select(
        F.col("cod_departamento").cast(IntegerType()).alias("cod_departamento"),
        F.col("nombre_municipio").alias("nombre_municipio"),
        F.col("cod_municipio").cast(IntegerType()).alias("cod_municipio"),
        F.lit("excel").alias("origen_divipola"),
    )

    # ── Municipios desde MySQL (si la tabla existe) ─────────────────────
    tabla_mysql = f"{catalog_name}.bronze.divipola_municipios_mysql"
    if spark.catalog.tableExists(tabla_mysql):
        df_mun_mysql = spark.table(tabla_mysql)
        df_mun_mysql = renombrar_si_existe(df_mun_mysql, {
            "codigodepartamento": "cod_departamento",
            "nombremunicipio":    "nombre_municipio",
            "municipio_id":       "cod_municipio",
        })
        df_mun_mysql = df_mun_mysql.select(
            F.col("cod_departamento").cast(IntegerType()).alias("cod_departamento"),
            F.col("nombre_municipio").alias("nombre_municipio"),
            F.col("cod_municipio").cast(IntegerType()).alias("cod_municipio"),
            F.lit("mysql").alias("origen_divipola"),
        )
        print(f"Municipios Excel: {df_mun_excel.count()}, MySQL: {df_mun_mysql.count()}")

        # Unir ambas fuentes - prioridad a Excel cuando hay duplicados
        # Estrategia: agregar MySQL solo los códigos que no estén en Excel
        codigos_excel = [r.cod_municipio for r in df_mun_excel.select("cod_municipio").collect()]
        df_mun_mysql_filtrado = df_mun_mysql.filter(~F.col("cod_municipio").isin(codigos_excel))
        df_municipios = df_mun_excel.unionByName(df_mun_mysql_filtrado)
        print(f"Municipios MySQL agregados (sin duplicar Excel): {df_mun_mysql_filtrado.count()}")
    else:
        df_municipios = df_mun_excel
        print("AVISO: tabla bronze.divipola_municipios_mysql no existe, solo se usa Excel")

    # ── Cruce final: municipios + departamentos ─────────────────────────
    df_divipola = (df_municipios
        .join(df_dept, on="cod_departamento", how="left")
        .select(
            "cod_departamento",
            "nombre_departamento",
            "cod_municipio",
            "nombre_municipio",
            "origen_divipola",
        )
        .filter(F.col("cod_municipio").isNotNull())
        .dropDuplicates(["cod_municipio"])
    )

    registros = df_divipola.count()

    (df_divipola.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(tabla_destino))

    print(f"OK ref.divipola: {registros} municipios consolidados")
    registrar_auditoria("nb_02_silver_transform", "ref", "divipola",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR ref.divipola: {msg}")
    registrar_auditoria("nb_02_silver_transform", "ref", "divipola",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: VIVIENDA
# MAGIC
# MAGIC Decodifica materiales, servicios públicos y separa dos conceptos:
# MAGIC - **riesgo_***: desastres naturales (P4065S*) — para dashboard "Condiciones de Vida"
# MAGIC - **problema_***: deterioros estructurales (P1891S*) — información complementaria
# MAGIC
# MAGIC La columna `REGION` se incluye condicionalmente (NULL para años que no la traen como 2017).

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.silver.vivienda"

try:
    df = spark.table(f"{catalog_name}.bronze.datos_vivienda")
    registros_in = df.count()

    df = renombrar_si_existe(df, {
        "DIRECTORIO":         "directorio",
        "CANT_HOG_COMPLETOS": "cant_hog_completos",
        "SECUENCIA_ENCUESTA": "secuencia_encuesta",
        "SECUENCIA_P":        "secuencia_p",
        "ORDEN":              "orden",

        # Características físicas
        "P1070":   "tipo_vivienda_cod",
        "P4005":   "material_paredes_cod",
        "P4015":   "material_pisos_cod",
        "P4567":   "material_techo_cod",

        # Servicios públicos
        "P8520S1": "energia_electrica_cod",
        "P8520S3": "alcantarillado_cod",
        "P8520S4": "recoleccion_basuras_cod",
        "P8520S5": "acueducto_cod",

        # Riesgos por desastres naturales (los REALES para el dashboard)
        "P4065S1": "riesgo_inundacion_cod",
        "P4065S2": "riesgo_avalancha_cod",
        "P4065S3": "riesgo_hundimiento_cod",
        "P4065S4": "riesgo_tormenta_cod",

        # Problemas estructurales (información complementaria)
        "P1891S1": "problema_humedades_cod",
        "P1891S2": "problema_goteras_cod",
        "P1891S3": "problema_grietas_paredes_cod",
        "P1891S4": "problema_grietas_piso_cod",
        "P1891S5": "problema_cielorrasos_cod",

        # Geografía
        "CLASE":   "clase_geografica_cod",
        "REGION":  "region_cod",

        # Otros
        "FEX_C":   "factor_expansion_raw",
    })

    # Decodificaciones (devuelven NULL si la columna fuente no existe)
    df = aplicar_diccionario(df, "tipo_vivienda_cod",    "tipo_vivienda",    TIPO_VIVIENDA)
    df = aplicar_diccionario(df, "material_paredes_cod", "material_paredes", MATERIAL_PAREDES)
    df = aplicar_diccionario(df, "material_pisos_cod",   "material_pisos",   MATERIAL_PISOS)
    df = aplicar_diccionario(df, "material_techo_cod",   "material_techo",   MATERIAL_TECHO)
    df = aplicar_diccionario(df, "clase_geografica_cod", "clase_geografica", CLASE_GEOGRAFICA)
    df = aplicar_diccionario(df, "region_cod",           "region",           REGION)

    # Booleanos: servicios públicos
    for cod, nuevo in [
        ("energia_electrica_cod",   "energia_electrica"),
        ("acueducto_cod",           "acueducto"),
        ("alcantarillado_cod",      "alcantarillado"),
        ("recoleccion_basuras_cod", "recoleccion_basuras"),
    ]:
        df = a_booleano_si_no(df, cod, nuevo)

    # Booleanos: riesgos por desastres
    for cod, nuevo in [
        ("riesgo_inundacion_cod",  "riesgo_inundacion"),
        ("riesgo_avalancha_cod",   "riesgo_avalancha"),
        ("riesgo_hundimiento_cod", "riesgo_hundimiento"),
        ("riesgo_tormenta_cod",    "riesgo_tormenta"),
    ]:
        df = a_booleano_si_no(df, cod, nuevo)

    # Booleanos: problemas estructurales
    for cod, nuevo in [
        ("problema_humedades_cod",         "problema_humedades"),
        ("problema_goteras_cod",           "problema_goteras"),
        ("problema_grietas_paredes_cod",   "problema_grietas_paredes"),
        ("problema_grietas_piso_cod",      "problema_grietas_piso"),
        ("problema_cielorrasos_cod",       "problema_cielorrasos"),
    ]:
        df = a_booleano_si_no(df, cod, nuevo)

    # Flags derivados
    df = df.withColumn(
        "sin_servicios_basicos",
        (~F.coalesce(F.col("energia_electrica"), F.lit(True)) |
         ~F.coalesce(F.col("acueducto"),         F.lit(True)) |
         ~F.coalesce(F.col("alcantarillado"),    F.lit(True)))
    ).withColumn(
        "vivienda_en_riesgo",
        F.coalesce(F.col("riesgo_inundacion"),  F.lit(False)) |
        F.coalesce(F.col("riesgo_avalancha"),   F.lit(False)) |
        F.coalesce(F.col("riesgo_hundimiento"), F.lit(False)) |
        F.coalesce(F.col("riesgo_tormenta"),    F.lit(False))
    )

    df = df.withColumn("factor_expansion", decimal_con_coma("factor_expansion_raw"))

    # Tipado de claves
    df = (df
        .withColumn("secuencia_encuesta", F.col("secuencia_encuesta").cast(IntegerType()))
        .withColumn("secuencia_p",        F.col("secuencia_p").cast(IntegerType()))
        .withColumn("orden",              F.col("orden").cast(IntegerType()))
        .withColumn("anio_encuesta",      F.col("anio_encuesta").cast(IntegerType())))

    df_silver = df.dropDuplicates(["directorio", "secuencia_encuesta", "anio_encuesta"])

    df_silver = df_silver.select(
        "directorio", "secuencia_encuesta", "secuencia_p", "orden",
        "cant_hog_completos", "anio_encuesta",
        "tipo_vivienda", "material_paredes", "material_pisos", "material_techo",
        "clase_geografica", "region",
        "energia_electrica", "acueducto", "alcantarillado", "recoleccion_basuras",
        "sin_servicios_basicos",
        "riesgo_inundacion", "riesgo_avalancha", "riesgo_hundimiento", "riesgo_tormenta",
        "vivienda_en_riesgo",
        "problema_humedades", "problema_goteras", "problema_grietas_paredes",
        "problema_grietas_piso", "problema_cielorrasos",
        "factor_expansion",
    )

    registros_out = df_silver.count()

    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    print(f"OK silver.vivienda: {registros_in} -> {registros_out}")
    registrar_auditoria("nb_02_silver_transform", "silver", "vivienda",
                        inicio, registros_in, registros_out, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR silver.vivienda: {msg}")
    registrar_auditoria("nb_02_silver_transform", "silver", "vivienda",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver: HOGAR
# MAGIC
# MAGIC Incluye decodificación de departamento/municipio de nacimiento vía DIVIPOLA.
# MAGIC Esto permite responder en Gold: "¿de dónde vienen los inmigrantes a cada región?"

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.silver.hogar"

try:
    df = spark.table(f"{catalog_name}.bronze.caract_comp_hogar")
    registros_in = df.count()

    df = renombrar_si_existe(df, {
        "DIRECTORIO":         "directorio",
        "SECUENCIA_ENCUESTA": "secuencia_encuesta",
        "SECUENCIA_P":        "secuencia_p",
        "ORDEN":              "orden",

        # Atributos demográficos
        "P6020":              "sexo_cod",
        "P6040":              "edad_raw",
        "P5502":              "estado_civil_cod",
        "P6080":              "raza_cod",

        # Satisfacciones (escala 0-10)
        "P1895":              "satisfaccion_vida_raw",
        "P1896":              "satisfaccion_ingresos_raw",
        "P1897":              "satisfaccion_salud_raw",
        "P1898":              "satisfaccion_seguridad_raw",
        "P1899":              "satisfaccion_trabajo_raw",

        # Lugar de nacimiento (desde 2018+ aparece con otro código, en 2017 es P6076)
        "P756":               "lugar_nacimiento_cod",
        "P756S1":             "cod_dept_nacimiento_raw",
        "P756S2":             "cod_mun_nacimiento_raw",

        # Otros
        "LLAVEHOG":           "llave_hogar",
        "FEX_C":              "factor_expansion_raw",
    })

    # Decodificaciones
    df = aplicar_diccionario(df, "sexo_cod",             "sexo",             SEXO)
    df = aplicar_diccionario(df, "estado_civil_cod",     "estado_civil",     ESTADO_CIVIL)
    df = aplicar_diccionario(df, "raza_cod",             "raza",             RAZA)
    df = aplicar_diccionario(df, "lugar_nacimiento_cod", "lugar_nacimiento", LUGAR_NACIMIENTO)

    # Tipado numérico
    if col_existe(df, "edad_raw"):
        df = df.withColumn("edad", F.col("edad_raw").cast(IntegerType()))
    for sat in ["vida", "ingresos", "salud", "seguridad", "trabajo"]:
        raw = f"satisfaccion_{sat}_raw"
        if col_existe(df, raw):
            df = df.withColumn(f"satisfaccion_{sat}", F.col(raw).cast(IntegerType()))
        else:
            df = df.withColumn(f"satisfaccion_{sat}", F.lit(None).cast(IntegerType()))

    df = df.withColumn("factor_expansion", decimal_con_coma("factor_expansion_raw"))

    # Códigos de departamento/municipio de nacimiento como enteros
    if col_existe(df, "cod_dept_nacimiento_raw"):
        df = df.withColumn("cod_dept_nacimiento", F.col("cod_dept_nacimiento_raw").cast(IntegerType()))
    else:
        df = df.withColumn("cod_dept_nacimiento", F.lit(None).cast(IntegerType()))
    if col_existe(df, "cod_mun_nacimiento_raw"):
        df = df.withColumn("cod_mun_nacimiento", F.col("cod_mun_nacimiento_raw").cast(IntegerType()))
    else:
        df = df.withColumn("cod_mun_nacimiento", F.lit(None).cast(IntegerType()))

    # Flags derivados de migración
    df = (df
        .withColumn("nacio_en_otro_pais",
            F.when(F.col("lugar_nacimiento_cod") == "3", F.lit(True))
             .when(F.col("lugar_nacimiento_cod").isNotNull(), F.lit(False))
             .otherwise(F.lit(None).cast(BooleanType())))
        .withColumn("nacio_en_otro_municipio",
            F.when(F.col("lugar_nacimiento_cod") == "2", F.lit(True))
             .when(F.col("lugar_nacimiento_cod").isNotNull(), F.lit(False))
             .otherwise(F.lit(None).cast(BooleanType())))
        .withColumn("es_migrante",
            F.coalesce(F.col("nacio_en_otro_pais"),     F.lit(False)) |
            F.coalesce(F.col("nacio_en_otro_municipio"), F.lit(False)))
    )

    # Rango etario
    df = df.withColumn(
        "rango_edad",
        F.when(F.col("edad") < 18, "0-17")
         .when(F.col("edad") < 30, "18-29")
         .when(F.col("edad") < 60, "30-59")
         .otherwise("60+")
    )

    # Enriquecimiento con DIVIPOLA (decodificar departamento/municipio de nacimiento)
    df_divipola = spark.table(f"{catalog_name}.ref.divipola").select(
        F.col("cod_municipio").alias("dvp_cod_municipio"),
        F.col("nombre_municipio").alias("nombre_mun_nacimiento"),
        F.col("nombre_departamento").alias("nombre_dept_nacimiento"),
    )

    df = df.join(df_divipola,
                 df.cod_mun_nacimiento == df_divipola.dvp_cod_municipio,
                 how="left") \
           .drop("dvp_cod_municipio")

    # Tipado de claves y deduplicación
    df = (df
        .withColumn("secuencia_encuesta", F.col("secuencia_encuesta").cast(IntegerType()))
        .withColumn("secuencia_p",        F.col("secuencia_p").cast(IntegerType()))
        .withColumn("orden",              F.col("orden").cast(IntegerType()))
        .withColumn("anio_encuesta",      F.col("anio_encuesta").cast(IntegerType())))

    df_silver = df.dropDuplicates(
        ["directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta"]
    )

    df_silver = df_silver.select(
        "directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta",
        "sexo", "edad", "rango_edad", "estado_civil", "raza", "llave_hogar",
        "satisfaccion_vida", "satisfaccion_ingresos", "satisfaccion_salud",
        "satisfaccion_seguridad", "satisfaccion_trabajo",
        "lugar_nacimiento",
        "cod_dept_nacimiento", "nombre_dept_nacimiento",
        "cod_mun_nacimiento",  "nombre_mun_nacimiento",
        "nacio_en_otro_pais", "nacio_en_otro_municipio", "es_migrante",
        "factor_expansion",
    )

    registros_out = df_silver.count()

    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    print(f"OK silver.hogar: {registros_in} -> {registros_out}")
    registrar_auditoria("nb_02_silver_transform", "silver", "hogar",
                        inicio, registros_in, registros_out, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR silver.hogar: {msg}")
    registrar_auditoria("nb_02_silver_transform", "silver", "hogar",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver: EDUCACIÓN

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.silver.educacion"

try:
    df = spark.table(f"{catalog_name}.bronze.educacion")
    registros_in = df.count()

    df = renombrar_si_existe(df, {
        "DIRECTORIO":         "directorio",
        "SECUENCIA_ENCUESTA": "secuencia_encuesta",
        "SECUENCIA_P":        "secuencia_p",
        "ORDEN":              "orden",

        "P6160":   "sabe_leer_escribir_cod",
        "P8586":   "actualmente_estudia_cod",
        "P8587":   "nivel_educativo_cod",
        "P8587S1": "grado_aprobado_raw",
        "P6211":   "anios_estudios_superiores_raw",
        "P6218":   "razon_no_estudiar_cod",
        "P5673":   "tipo_establecimiento_cod",
        "P1101":   "jornada_educativa_cod",

        # Beca
        "P8610":   "recibio_beca_cod",
        "P8610S1": "valor_beca_raw",
        "P6229":   "entidad_beca_cod",

        # Subsidio educativo (distinto de beca)
        "P8612":   "recibio_subsidio_cod",
        "P8612S1": "valor_subsidio_raw",

        # Crédito educativo
        "P8614":   "recibio_credito_cod",
        "P8614S1": "valor_credito_raw",

        # Otros
        "LLAVEHOG": "llave_hogar",
        "FEX_C":    "factor_expansion_raw",
    })

    # Decodificaciones
    df = aplicar_diccionario(df, "nivel_educativo_cod",      "nivel_educativo",      NIVEL_EDUCATIVO)
    df = aplicar_diccionario(df, "razon_no_estudiar_cod",    "razon_no_estudiar",    RAZON_NO_ESTUDIAR)
    df = aplicar_diccionario(df, "tipo_establecimiento_cod", "tipo_establecimiento", TIPO_ESTABLECIMIENTO)
    df = aplicar_diccionario(df, "jornada_educativa_cod",    "jornada_educativa",    JORNADA_EDUCATIVA)
    df = aplicar_diccionario(df, "entidad_beca_cod",         "entidad_beca",         ENTIDAD_BECA)

    # Booleanos
    df = a_booleano_si_no(df, "sabe_leer_escribir_cod",  "sabe_leer_escribir")
    df = a_booleano_si_no(df, "actualmente_estudia_cod", "actualmente_estudia")
    df = a_booleano_si_no(df, "recibio_beca_cod",        "recibio_beca")
    df = a_booleano_si_no(df, "recibio_subsidio_cod",    "recibio_subsidio")
    df = a_booleano_si_no(df, "recibio_credito_cod",     "recibio_credito")

    # Flag derivado: recibió cualquier apoyo económico
    df = df.withColumn(
        "recibio_apoyo_educativo",
        F.coalesce(F.col("recibio_beca"),     F.lit(False)) |
        F.coalesce(F.col("recibio_subsidio"), F.lit(False)) |
        F.coalesce(F.col("recibio_credito"),  F.lit(False))
    )

    # Valores monetarios
    df = (df
        .withColumn("valor_beca",     F.coalesce(valor_monetario_con_coma("valor_beca_raw"),     F.lit(0).cast(DecimalType(14, 2))))
        .withColumn("valor_subsidio", F.coalesce(valor_monetario_con_coma("valor_subsidio_raw"), F.lit(0).cast(DecimalType(14, 2))))
        .withColumn("valor_credito",  F.coalesce(valor_monetario_con_coma("valor_credito_raw"),  F.lit(0).cast(DecimalType(14, 2))))
    )

    # Numéricos
    df = (df
        .withColumn("grado_aprobado",            F.col("grado_aprobado_raw").cast(IntegerType()))
        .withColumn("anios_estudios_superiores", F.col("anios_estudios_superiores_raw").cast(IntegerType()))
        .withColumn("factor_expansion",          decimal_con_coma("factor_expansion_raw"))
    )

    # Tipado de claves y deduplicación
    df = (df
        .withColumn("secuencia_encuesta", F.col("secuencia_encuesta").cast(IntegerType()))
        .withColumn("secuencia_p",        F.col("secuencia_p").cast(IntegerType()))
        .withColumn("orden",              F.col("orden").cast(IntegerType()))
        .withColumn("anio_encuesta",      F.col("anio_encuesta").cast(IntegerType())))

    df_silver = df.dropDuplicates(
        ["directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta"]
    )

    df_silver = df_silver.select(
        "directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta",
        "sabe_leer_escribir", "actualmente_estudia",
        "nivel_educativo", "grado_aprobado", "anios_estudios_superiores",
        "razon_no_estudiar",
        "tipo_establecimiento", "jornada_educativa",
        "recibio_beca",     "valor_beca",     "entidad_beca",
        "recibio_subsidio", "valor_subsidio",
        "recibio_credito",  "valor_credito",
        "recibio_apoyo_educativo",
        "llave_hogar", "factor_expansion",
    )

    registros_out = df_silver.count()

    (df_silver.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    print(f"OK silver.educacion: {registros_in} -> {registros_out}")
    registrar_auditoria("nb_02_silver_transform", "silver", "educacion",
                        inicio, registros_in, registros_out, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR silver.educacion: {msg}")
    registrar_auditoria("nb_02_silver_transform", "silver", "educacion",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver: PERSONA (consolidado)
# MAGIC
# MAGIC Vista persona unificada que combina hogar + educación + ubicación de la vivienda.
# MAGIC Es la base directa de `gold.dim_persona`.

# COMMAND ----------

inicio = utc_now()
tabla_destino = f"{catalog_name}.silver.persona"

try:
    df_hogar    = spark.table(f"{catalog_name}.silver.hogar")
    df_edu      = spark.table(f"{catalog_name}.silver.educacion")
    df_vivienda = spark.table(f"{catalog_name}.silver.vivienda")

    claves_persona = ["directorio", "secuencia_encuesta", "secuencia_p", "orden", "anio_encuesta"]
    # NOTA TÉCNICA: La clave natural de la vivienda es (directorio, anio_encuesta).
    # 'secuencia_encuesta' identifica al HOGAR dentro de la vivienda (1..24),
    # NO la vivienda física. Una vivienda puede contener varios hogares.
    claves_vivienda = ["directorio", "anio_encuesta"]

    # Subset educación (excluyendo columnas que ya están en hogar)
    df_edu_subset = df_edu.select(
        *claves_persona,
        "sabe_leer_escribir", "actualmente_estudia",
        "nivel_educativo", "grado_aprobado", "anios_estudios_superiores",
        "razon_no_estudiar", "tipo_establecimiento", "jornada_educativa",
        "recibio_beca", "valor_beca", "entidad_beca",
        "recibio_subsidio", "valor_subsidio",
        "recibio_credito", "valor_credito",
        "recibio_apoyo_educativo",
    )

    # Subset vivienda (solo la región y clase para propagar a persona)
    df_viv_subset = df_vivienda.select(
        *claves_vivienda,
        "region", "clase_geografica"
    )

    # Join: hogar + educación
    df_persona = df_hogar.join(df_edu_subset, on=claves_persona, how="left")

    # Join: + vivienda (para propagar región/clase)
    df_persona = df_persona.join(df_viv_subset, on=claves_vivienda, how="left")

    (df_persona.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("anio_encuesta")
        .saveAsTable(tabla_destino))

    registros = df_persona.count()
    print(f"OK silver.persona: {registros} registros")
    registrar_auditoria("nb_02_silver_transform", "silver", "persona",
                        inicio, registros, registros, "OK")
except Exception as e:
    msg = str(e)[:500]
    print(f"ERR silver.persona: {msg}")
    registrar_auditoria("nb_02_silver_transform", "silver", "persona",
                        inicio, 0, 0, "ERROR", msg)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Resumen de la corrida

# COMMAND ----------

df_resumen = (
    spark.table(f"{catalog_name}.audit.pipeline_runs")
    .filter(F.col("run_id") == run_id)
    .select("tabla_destino", "estado", "registros_in", "registros_out", "duracion_seg")
    .orderBy("inicio_utc")
)
display(df_resumen)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validaciones de calidad

# COMMAND ----------

print("=" * 75)
print("VALIDACIONES — CATÁLOGOS VERIFICADOS CON DICCIONARIO OFICIAL DANE")
print("=" * 75)

print("\n=== DIVIPOLA consolidado ===")
df_dvp = spark.table(f"{catalog_name}.ref.divipola")
print(f"Total municipios: {df_dvp.count()}")
print("Distribución por origen:")
df_dvp.groupBy("origen_divipola").count().show()
print("Top 5 departamentos con más municipios:")
df_dvp.groupBy("nombre_departamento").count().orderBy("count", ascending=False).show(5, truncate=False)

print("\n=== MÓDULO VIVIENDA ===\n")
print("TIPO DE VIVIENDA:")
spark.table(f"{catalog_name}.silver.vivienda").groupBy("tipo_vivienda").count().orderBy("count", ascending=False).show(truncate=False)

print("REGION (NULL = años sin REGION como 2017):")
spark.table(f"{catalog_name}.silver.vivienda").groupBy("region").count().show(truncate=False)

print("Viviendas en riesgo de algún desastre:")
spark.table(f"{catalog_name}.silver.vivienda").groupBy("vivienda_en_riesgo").count().show()

print("\n=== MÓDULO HOGAR ===\n")
print("LUGAR DE NACIMIENTO:")
spark.table(f"{catalog_name}.silver.hogar").groupBy("lugar_nacimiento").count().orderBy("count", ascending=False).show(truncate=False)

print("¿Es migrante? (nacido en otro municipio o país):")
spark.table(f"{catalog_name}.silver.hogar").groupBy("es_migrante").count().show()

print("Top 10 departamentos de nacimiento (de migrantes):")
(spark.table(f"{catalog_name}.silver.hogar")
    .filter(F.col("nombre_dept_nacimiento").isNotNull())
    .groupBy("nombre_dept_nacimiento").count()
    .orderBy("count", ascending=False).show(10, truncate=False))

print("\n=== MÓDULO EDUCACIÓN ===\n")
print("NIVEL EDUCATIVO:")
spark.table(f"{catalog_name}.silver.educacion").groupBy("nivel_educativo").count().orderBy("count", ascending=False).show(truncate=False)

print("APOYOS ECONÓMICOS (3 conceptos separados según DANE):")
print("Recibió BECA (P8610):")
spark.table(f"{catalog_name}.silver.educacion").groupBy("recibio_beca").count().show()
print("Recibió SUBSIDIO (P8612):")
spark.table(f"{catalog_name}.silver.educacion").groupBy("recibio_subsidio").count().show()
print("Recibió CRÉDITO (P8614):")
spark.table(f"{catalog_name}.silver.educacion").groupBy("recibio_credito").count().show()

# COMMAND ----------

dbutils.notebook.exit(f"SILVER_OK run_id={run_id}")
