# Pipeline Medallón — Encuesta de Calidad de Vida (ECV) DANE Colombia

Arquitectura Medallion en Azure Databricks con orquestación híbrida ADF + Databricks Workflows y CI/CD completo.

Pipeline ETL que transforma los microdatos públicos de la **Encuesta de Calidad de Vida (ECV) del DANE Colombia** (2017-2018), implementando la Arquitectura Medallion (Bronze → Silver → Gold) en Azure Databricks con Unity Catalog, modelo dimensional estrella y despliegue automatizado via GitHub Actions.

---

## 🎯 Descripción

Este proyecto migra un trabajo académico de BI originalmente construido con SQL Server + SSIS + SSAS (2021) a una **arquitectura medallón moderna en Azure**, demostrando:

- Ingesta multi-fuente heterogénea (CSV + Excel + MySQL)
- Transformación y limpieza con PySpark 
- Modelo dimensional Kimball con 5 dimensiones y tabla de hechos
- Serving layer hacia Azure SQL Database para consumo desde Power BI
- Orquestación híbrida Azure Data Factory + Databricks Workflows
- CI/CD con GitHub Actions para deploy automático dev → prod
- Validación exhaustiva contra la documentación oficial del DANE

---

## ✨ Características Principales

- 🔄 **ETL Multi-Fuente** — 3 orígenes: CSV (DANE), Excel (DIVIPOLA), MySQL (DIVIPOLA complementario)
- 🏗️ **Arquitectura Medallion** — Bronze (raw) → Silver (clean) → Gold (dimensional) → Serving (Azure SQL)
- 📊 **Modelo Dimensional** — Star Schema Kimball con 5 dimensiones + tabla de hechos (309.512 registros)
- 🔐 **Unity Catalog** — Gobernanza con catálogos separados dev/prod, allowlist Maven gestionada
- 🔑 **Azure Key Vault** — Gestión centralizada de secretos (zero credentials in code)
- 🏭 **Orquestación Híbrida** — Azure Data Factory (macro) + Databricks Workflows (interno)
- 🚀 **CI/CD Completo** — GitHub Actions despliega notebooks + workflows + ADF automáticamente
- ⚡ **Delta Lake** — ACID transactions, schema evolution, particionamiento por año
- 📈 **Power BI** — Dashboards de Educación y Condiciones de Vida conectados a Azure SQL
- ✅ **Validación DANE** — 14 catálogos verificados contra el diccionario oficial de microdatos

---

## 🏛️ Arquitectura

### Flujo de Datos End-to-End

```
┌─────────────────┐     ┌──────────────────────────────────────────────────┐
│   Fuentes Raw    │     │              Azure Databricks                    │
│                  │     │                                                  │
│  📄 CSV (DANE)  │────►│  🥉 Bronze ──► 🥈 Silver ──► 🥇 Gold           │
│  📊 Excel       │     │  (Extract)     (Transform)    (Load)             │
│  🗄️ MySQL      │     │                                                  │
└─────────────────┘     └──────────────────────┬───────────────────────────┘
                                               │
                        ┌──────────────────────▼───────────────────────────┐
                        │              Serving Layer                        │
                        │                                                  │
                        │  🗄️ Azure SQL Database ──► 📊 Power BI          │
                        └──────────────────────────────────────────────────┘
```

<img width="2816" height="1536" alt="Arquitectura" src="https://github.com/user-attachments/assets/e79a0030-24e3-4e19-b709-777d9aa6d188" />



### Servicios Azure Utilizados (10 servicios)

| # | Servicio | Rol en el proyecto |
|---|----------|-------------------|
| 1 | **Azure Databricks** | Motor de procesamiento PySpark |
| 2 | **Unity Catalog** | Gobernanza de datos (catálogos dev/prod) |
| 3 | **Azure Data Lake Gen2** | Almacenamiento de capas medallón |
| 4 | **Azure Key Vault** | Gestión centralizada de secretos |
| 5 | **Azure Database for MySQL** | 3ª fuente de datos (DIVIPOLA) |
| 6 | **Azure SQL Database** | Serving layer para Power BI |
| 7 | **Azure Data Factory** | Orquestación macro del pipeline |
| 8 | **Databricks Workflows** | Orquestación interna de notebooks |
| 9 | **GitHub Actions** | CI/CD automatizado |
| 10 | **Power BI** | Visualización y dashboards |

### Orquestación Híbrida

El proyecto implementa un patrón **Orchestrator-of-Orchestrators**:

```
Azure Data Factory (pl_ecv_master)
├── 1. Validar archivos raw en Blob Storage
├── 2. Disparar Databricks Workflow ──────────┐
├── 3. Validar serving en Azure SQL            │
└── 4. Registrar resultado                     │
                                               ▼
                              Databricks Workflow (pl_ecv_databricks)
                              ├── nb_00_prep_amb
                              ├── nb_01_bronze_ingesta
                              ├── nb_02_silver_transform
                              ├── nb_03_gold_dimensiones
                              ├── nb_04_gold_hechos
                              └── nb_05_serving
```

ADF coordina servicios heterogéneos (Storage, Databricks, Azure SQL) mientras que Databricks Workflows orquesta internamente las 6 tareas PySpark.

![Pipeline ADF]
<img width="793" height="234" alt="image" src="https://github.com/user-attachments/assets/c1daef95-2ae7-4e19-981b-c476c1ce685f" />


![Workflow Databricks]
<img width="809" height="268" alt="image" src="https://github.com/user-attachments/assets/77f09b23-d45f-4398-8967-93f0583d419b" />


---

## 📁 Datasets

### Fuentes de datos

El proyecto consume datos de **3 fuentes heterogéneas**:

| Fuente | Tipo | Descripción | Registros |
|--------|------|-------------|-----------|
| **ECV 2017** | CSV (ADLS Gen2) | Encuesta Calidad de Vida DANE — 3 archivos | 59.649 |
| **ECV 2018** | CSV (ADLS Gen2) | Encuesta Calidad de Vida DANE — 3 archivos | 632.560 |
| **DIVIPOLA Excel** | Excel (ADLS Gen2) | Maestro departamentos y municipios | 532 |
| **DIVIPOLA MySQL** | MySQL (Azure DB) | Municipios complementarios | 623 |
| **Total** | | | **693.364** |

### Archivos CSV del DANE por año

| Archivo | Descripción | 2017 | 2018 |
|---------|-------------|------|------|
| `Caracteristicas_y_composicion_del_hogar` | Datos demográficos por persona | 26.500 | 283.012 |
| `Datos_de_la_vivienda_Actualizada` | Características físicas de viviendas | 8.501 | 88.713 |
| `Educacion` | Variables educativas por persona | 24.648 | 260.835 |

Fuente oficial: [DANE — Microdatos ECV](https://microdatos.dane.gov.co/index.php/catalog/544)

### Catálogo DIVIPOLA consolidado

Se consolidaron **1.122 municipios** de Colombia desde 2 fuentes:
- **Excel**: 499 municipios (maestro DIVIPOLA del DANE)
- **MySQL**: 623 municipios complementarios

La consolidación usa anti-join para evitar duplicados, priorizando Excel como fuente primaria.

---

## 🏗️ Capas del Pipeline

### 🥉 Capa Bronze (Extract)

Ingesta cruda sin transformaciones. Los datos llegan tal cual de las fuentes.

- **Modo de escritura**: `append` (permite carga multi-año incremental)
- **Formato**: Delta Lake con schema evolution (`mergeSchema=true`)
- **Particionamiento**: por `anio_encuesta`
- **Normalización**: nombres de columnas compatibles con Delta (sin tildes, espacios)

| Tabla Bronze | Fuente | Registros |
|-------------|--------|-----------|
| `caract_comp_hogar` | CSV DANE | 309.512 |
| `datos_vivienda` | CSV DANE | 97.214 |
| `educacion` | CSV DANE | 285.483 |
| `divipola_departamentos` | Excel | 33 |
| `divipola_municipios_excel` | Excel | 499 |
| `divipola_municipios_mysql` | MySQL | 623 |

### 🥈 Capa Silver (Transform)

Limpieza, decodificación de catálogos y conformado de datos. **14 catálogos validados** contra la documentación oficial del DANE.

- **Modo de escritura**: `overwrite` (idempotente)
- **Decodificaciones**: tipo vivienda, materiales, servicios públicos, estado civil, nivel educativo, etc.
- **Flags derivados**: `es_migrante`, `vivienda_en_riesgo`, `sin_servicios_basicos`, `recibio_apoyo_educativo`
- **Enriquecimiento**: lugar de nacimiento decodificado vía DIVIPOLA (100% cobertura)

| Tabla Silver | Descripción | Registros |
|-------------|-------------|-----------|
| `ref.divipola` | Catálogo geográfico consolidado | 1.122 |
| `silver.vivienda` | Viviendas con servicios y riesgos | 97.214 |
| `silver.hogar` | Personas con demografía y migración | 309.512 |
| `silver.educacion` | Registros educativos con apoyos | 285.483 |
| `silver.persona` | Vista consolidada persona | 309.512 |

### 🥇 Capa Gold (Load)

Modelo dimensional estrella optimizado para análisis.

- **Surrogate Keys**: SHA-256 truncado a 15 hex chars (BIGINT-safe, deterministas)
- **Integridad referencial**: 100% verificada post-carga
- **FKs nullables**: `sk_educacion` NULL para menores de 5 años, `sk_ubicacion` NULL para 2017

```
                          ┌─────────────────┐
                          │    fac_ecv      │
                          │                 │
  ┌────────────────┐      │                 │      ┌────────────────┐
  │  dim_tiempo    │◄─────┤ sk_tiempo       │      │ dim_ubicacion  │
  │                │      │ sk_persona  ────┼─────►│                │
  └────────────────┘      │ sk_vivienda ────┼──┐   └────────────────┘
                          │ sk_educacion────┼┐ │
  ┌────────────────┐      │ sk_ubicacion    ││ │   ┌────────────────┐
  │  dim_persona   │◄─────┤                 ││ └──►│ dim_vivienda   │
  │                │      │ + métricas      ││     │                │
  └────────────────┘      └─────────────────┘│     └────────────────┘
                                             │
                          ┌──────────────────┘
                          │
                          ▼
                   ┌────────────────┐
                   │ dim_educacion  │
                   │                │
                   └────────────────┘
```

| Tabla Gold | Tipo | Registros | Descripción |
|-----------|------|-----------|-------------|
| `dim_persona` | Dimensión | 309.512 | Demografía + migración + bienestar |
| `dim_vivienda` | Dimensión | 97.214 | Materiales + servicios + riesgos |
| `dim_educacion` | Dimensión | 285.483 | Niveles + apoyos económicos |
| `dim_ubicacion` | Dimensión | 9 | Regiones DANE |
| `dim_tiempo` | Dimensión | 2 | Años de encuesta |
| `fac_ecv` | Hechos | 309.512 | Métricas + FKs a dimensiones |
| `kpi_becas_anuales` | Agregado | 2 | KPIs de becas por año |
| `kpi_condiciones_vida_region` | Agregado | 9 | KPIs de vivienda por región |

### 🔌 Serving Layer (Azure SQL)

Sincronización Gold → Azure SQL Database para consumo desde Power BI.

- **8 tablas** sincronizadas con verificación de conteos post-carga
- **Modo**: `overwrite` con `truncate=true` (idempotente)
- **Performance**: ~18 minutos en Azure SQL Basic (5 DTUs)

---

## 🔬 Hallazgos Técnicos

Durante la construcción del proyecto se descubrieron hallazgos significativos que enriquecen el análisis:

### 1. Validación contra documentación oficial DANE
Se verificaron 14 catálogos contra el diccionario oficial publicado en `microdatos.dane.gov.co/catalog/544`. Se identificaron **mapeos incorrectos** en el proyecto original 2021 (estado civil, materiales de construcción, variables educativas).

### 2. REGION solo disponible desde 2018
Los CSV públicos de la ECV 2017 **no contienen** la variable REGION (limitación de la Resolución 173 de 2008 sobre reserva estadística). A partir de 2018 sí aparece la variable V5927 con las 9 regiones del DANE.

### 3. Grano de vivienda vs hogar
La columna `secuencia_encuesta` identifica al **hogar** dentro de la vivienda (puede tomar valores 1 a 24), no la vivienda física. La clave natural correcta de la vivienda es `(directorio, anio_encuesta)`.

### 4. Menores de 5 años sin registro educativo
Los 24.029 menores de 5 años no están en el módulo Educación del DANE (por diseño del cuestionario). Se modeló con FK nullable (`sk_educacion = NULL`) mediante lookup explícito.

### 5. Surrogate keys 15-hex para BIGINT
SHA-256 truncado a 15 caracteres hexadecimales (no 16) para garantizar compatibilidad con BIGINT signed de Spark (2^60 valores, sin riesgo de overflow).

### 6. Bug del driver MariaDB con MySQL 8.0
El driver JDBC MariaDB preinstalado en Databricks Runtime retornaba metadata en lugar de datos al leer columnas de MySQL 8.0. Se resolvió instalando el driver oficial MySQL Connector/J (`com.mysql:mysql-connector-j:8.4.0`) gestionado vía allowlist de Unity Catalog.

### 7. SQL Server no permite ORDER BY en subqueries
Las cláusulas ORDER BY no son permitidas en subqueries de SQL Server sin TOP/OFFSET. Se resolvió moviendo el ordenamiento al lado de Spark (`DataFrame.orderBy()`).

### 8. Consolidación multi-fuente sin duplicados
DIVIPOLA consolidado desde Excel (499) + MySQL (623) = 1.122 municipios. Anti-join por `cod_municipio` evita duplicados priorizando Excel como fuente primaria.

---


## 🚀 CI/CD

### Estrategia de Despliegue

El proyecto implementa **2 workflows de GitHub Actions**:

```
GitHub Actions
├── Deploy Databricks ──► Notebooks + Workflow a workspace prod
│   - Exporta notebooks desde repo
│   - Importa a carpeta prod en Databricks
│   - Crea workflow WF_PROD_ECV_DANE
│   - Ejecuta y monitorea
│
└── Deploy ADF ──► Pipeline + Linked Services a adf-ecv-prod
    - ARM Templates desde rama adf_publish
    - Secretos inyectados desde GitHub Secrets
    - Parámetros específicos por ambiente
```

### Secrets Configurados

| Secret | Descripción |
|--------|-------------|
| `DATABRICKS_HOST` | URL del workspace Databricks |
| `DATABRICKS_TOKEN` | Personal Access Token de Databricks |
| `AZURE_CREDENTIALS` | Service Principal para deploy ARM |
| `ADLS_ACCOUNT_KEY` | Key del Storage Account |

### Separación de Ambientes

| Componente | Desarrollo | Producción |
|-----------|------------|------------|
| Catálogo Unity | `ecv_dev` | `ecv_prod` |
| Azure SQL Database | `ecv-dev` | `ecv-prod` |
| Azure Data Factory | `adf-ecv-dev` | `adf-ecv-prod` |
| Databricks Workflow | `pl_ecv_databricks` | `WF_PROD_ECV_DANE` |
| Notebooks folder | `/proyecto-smartdata/proceso/` | `/proyecto-smartdata-prod/proceso/` |

<img width="1919" height="731" alt="Captura de pantalla 2026-06-06 173232" src="https://github.com/user-attachments/assets/6874a9ad-6149-4546-90e8-51878e48971a" />


---

## 📈 Dashboards

### Dashboard Educación

Análisis de cobertura educativa, distribución por niveles y apoyos económicos (becas, subsidios, créditos).



### Dashboard Condiciones de Vida

Análisis de condiciones habitacionales por región: servicios públicos, riesgos de desastres naturales, materiales de construcción.

<img width="709" height="430" alt="image" src="https://github.com/user-attachments/assets/573c0405-5618-4146-9c41-45b794079873" />
<img width="712" height="404" alt="image" src="https://github.com/user-attachments/assets/b66df58e-2459-4774-ab63-1eece24f91f8" />


---

## 💻 Instalación y Configuración

### Prerrequisitos

- Azure Subscription con créditos disponibles
- Azure Databricks Premium (Unity Catalog requiere Premium)
- Node.js 18+ (para CI/CD)
- Power BI Desktop (para visualización)
- MySQL Workbench (para carga de DIVIPOLA)

### 1️⃣ Crear Recursos Azure

```
- Storage Account (ADLS Gen2) con contenedores: raw, bronze, silver, gold
- Azure Databricks Premium workspace
- Azure Key Vault con secretos de conexión
- Azure Database for MySQL Flexible Server
- Azure SQL Database (Basic tier)
- Azure Data Factory V2
```

### 2️⃣ Configurar Unity Catalog

Ejecutar el notebook `nb_00_prep_amb` que crea automáticamente:

```
- Catálogos: ecv_dev, ecv_prod
- Esquemas: bronze, silver, gold, ref, audit
- Storage Credential con Managed Identity
- Tablas de auditoría: pipeline_runs, carga_anual
```

### 3️⃣ Subir Datos Raw

Subir los archivos CSV del DANE al contenedor `raw` del Storage Account:

```
raw/
└── ecv/
    ├── 2017/
    │   ├── Caracteristicas_y_composicion_del_hogar2017.csv
    │   ├── Datos_de_la_vivienda_Actualizada2017.csv
    │   └── Educacion2017.csv
    ├── 2018/
    │   ├── Caracteristicas_y_composicion_del_hogar2018.csv
    │   ├── Datos_de_la_vivienda_Actualizada2018.csv
    │   └── Educacion2018.csv
    └── divipola/
        └── Departamentos_y_Municipios.xls
```

### 4️⃣ Configurar GitHub Secrets

| Secret | Valor |
|--------|-------|
| `DATABRICKS_HOST` | `https://adb-XXXXXX.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Token PAT de Databricks |
| `AZURE_CREDENTIALS` | JSON del Service Principal |
| `ADLS_ACCOUNT_KEY` | Key del Storage Account |

### 5️⃣ Ejecutar Pipeline

```bash
# Opción A: Desde GitHub Actions (recomendado)
git push origin main    # Dispara deploy automático

# Opción B: Desde Databricks
# Workflows → pl_ecv_databricks → Run now

# Opción C: Desde Azure Data Factory
# pl_ecv_master → Trigger now
```

---

## 📁 Estructura del Proyecto

```
proyecto-smartdata/
├── .github/
│   └── workflow/
│       ├── deploy.yml                    ← CI/CD Databricks (notebooks + workflow)
│       └── deploy_adf.yml               ← CI/CD Azure Data Factory (ARM templates)
│
├── proceso/                              ← Notebooks PySpark del ETL
│   ├── nb_00_setup.py                    ← Preparación de ambiente
│   ├── nb_01_bronze_ingesta_v4.py        ← Extract (3 fuentes)
│   ├── nb_02_silver_transform_v3.py      ← Transform (14 catálogos)
│   ├── nb_03_gold_dimensiones_v3.py      ← Load (5 dimensiones)
│   ├── nb_04_gold_hechos_v2.py           ← Load (tabla de hechos + KPIs)
│   └── nb_05_serving.py                  ← Serving (Gold → Azure SQL)
│
├── PrepAmb/                              ← Scripts SQL de preparación
│   └── create_catalog_schemas.sql
│
├── seguridad/                            ← Scripts SQL de permisos
│   └── grants.sql
│
├── reversion/                            ← Scripts SQL de limpieza
│   └── drop_all.sql
│
├── datasets/                             ← Descripción de datasets e insumos
│
├── dashboard/                            ← Archivos Power BI (.pbix, capturas)
│
├── certificaciones/                      ← Certificaciones obtenidas
│
├── evidencias/                           ← Capturas de pantalla
│   ├── github_actions_deploy.png
│   ├── databricks_workflow_verde.png
│   ├── adf_pipeline_master.png
│   ├── azure_resource_group.png
│   └── ...
│
└── README.md                             ← Este archivo
```

---


---

## Stack Tecnológico

| Categoría | Tecnología |
|-----------|-----------|
| **Procesamiento** | PySpark (DataFrame API puro) |
| **Almacenamiento** | Delta Lake con Unity Catalog |
| **Gobernanza** | Unity Catalog (catálogos dev/prod) |
| **Storage** | Azure Data Lake Gen2 (ADLS) |
| **Orquestación** | Azure Data Factory + Databricks Workflows |
| **Secretos** | Azure Key Vault |
| **Base de datos** | Azure SQL Database + Azure MySQL |
| **Visualización** | Power BI Desktop |
| **CI/CD** | GitHub Actions |
| **Versionamiento** | Git + GitHub |

---

## 👤 Autor

### Eduar Alonso Caro Montoya

**Data Engineering** | **Azure Databricks** | **Delta Lake** | **CI/CD**

---

## 📄 Licencia

Este proyecto fue desarrollado como trabajo final de Ingeniería de Datos con Databricks.
