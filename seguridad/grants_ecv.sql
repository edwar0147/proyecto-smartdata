-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Grants — Proyecto ECV Calidad de Vida DANE Colombia
-- MAGIC
-- MAGIC Script de seguridad que define los permisos por grupo y usuario sobre los
-- MAGIC catálogos, esquemas y tablas del proyecto. Sigue el principio de **mínimo privilegio**:
-- MAGIC cada rol solo tiene acceso a lo que necesita para su función.
-- MAGIC
-- MAGIC ### Roles definidos
-- MAGIC
-- MAGIC | Grupo | Rol | Acceso |
-- MAGIC |-------|-----|--------|
-- MAGIC | Arquitectos_Datos | Diseño y gobernanza | Full admin sobre catálogos y schemas |
-- MAGIC | Ingenieros_Datos | Construcción del pipeline ETL | CRUD en bronze, silver, gold |
-- MAGIC | Analistas_BI | Consumo de datos para dashboards | SELECT en gold y ref |
-- MAGIC | Desarrolladores | Desarrollo y pruebas | SELECT en silver y gold, CREATE en bronze |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Creación de grupos

-- COMMAND ----------

CREATE GROUP IF NOT EXISTS `Arquitectos_Datos`;
CREATE GROUP IF NOT EXISTS `Ingenieros_Datos`;
CREATE GROUP IF NOT EXISTS `Analistas_BI`;
CREATE GROUP IF NOT EXISTS `Desarrolladores`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Asignación de usuarios a grupos

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Arquitectos de Datos (diseño, gobernanza, administración)
-- ═══════════════════════════════════════════════════════════════
ALTER GROUP `Arquitectos_Datos`
ADD USER `carlos.mendez@ecvdaneproject.onmicrosoft.com`;

ALTER GROUP `Arquitectos_Datos`
ADD USER `diana.restrepo@ecvdaneproject.onmicrosoft.com`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Ingenieros de Datos (construcción y mantenimiento del pipeline)
-- ═══════════════════════════════════════════════════════════════
ALTER GROUP `Ingenieros_Datos`
ADD USER `andres.garcia@ecvdaneproject.onmicrosoft.com`;

ALTER GROUP `Ingenieros_Datos`
ADD USER `laura.martinez@ecvdaneproject.onmicrosoft.com`;

ALTER GROUP `Ingenieros_Datos`
ADD USER `felipe.ruiz@ecvdaneproject.onmicrosoft.com`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Analistas de BI (consumo de datos, dashboards, reportes)
-- ═══════════════════════════════════════════════════════════════
ALTER GROUP `Analistas_BI`
ADD USER `maria.lopez@ecvdaneproject.onmicrosoft.com`;

ALTER GROUP `Analistas_BI`
ADD USER `jorge.herrera@ecvdaneproject.onmicrosoft.com`;

ALTER GROUP `Analistas_BI`
ADD USER `valentina.rojas@ecvdaneproject.onmicrosoft.com`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Desarrolladores (desarrollo, pruebas, soporte)
-- ═══════════════════════════════════════════════════════════════
ALTER GROUP `Desarrolladores`
ADD USER `santiago.perez@ecvdaneproject.onmicrosoft.com`;

ALTER GROUP `Desarrolladores`
ADD USER `camila.torres@ecvdaneproject.onmicrosoft.com`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 3. Acceso al catálogo DEV

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Catálogo ecv_dev — Ambiente de desarrollo
-- ═══════════════════════════════════════════════════════════════

-- Todos los grupos necesitan USE CATALOG para acceder
GRANT USE CATALOG ON CATALOG ecv_dev TO `Arquitectos_Datos`;
GRANT USE CATALOG ON CATALOG ecv_dev TO `Ingenieros_Datos`;
GRANT USE CATALOG ON CATALOG ecv_dev TO `Analistas_BI`;
GRANT USE CATALOG ON CATALOG ecv_dev TO `Desarrolladores`;

-- Arquitectos tienen control total del catálogo dev
GRANT ALL PRIVILEGES ON CATALOG ecv_dev TO `Arquitectos_Datos`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 4. Acceso al catálogo PROD

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Catálogo ecv_prod — Ambiente de producción
-- Solo Arquitectos e Ingenieros pueden escribir en prod
-- Analistas solo leen de gold
-- Desarrolladores NO tienen acceso a prod
-- ═══════════════════════════════════════════════════════════════

GRANT USE CATALOG ON CATALOG ecv_prod TO `Arquitectos_Datos`;
GRANT USE CATALOG ON CATALOG ecv_prod TO `Ingenieros_Datos`;
GRANT USE CATALOG ON CATALOG ecv_prod TO `Analistas_BI`;
-- Nota: Desarrolladores NO tienen acceso a producción (por diseño)

-- Arquitectos tienen control total del catálogo prod
GRANT ALL PRIVILEGES ON CATALOG ecv_prod TO `Arquitectos_Datos`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 5. Acceso a schemas — ecv_dev

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_dev.bronze (datos crudos ingestados)
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_dev.bronze TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_dev.bronze TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_dev.bronze TO `Ingenieros_Datos`;

GRANT USE SCHEMA ON SCHEMA ecv_dev.bronze TO `Desarrolladores`;
GRANT CREATE TABLE ON SCHEMA ecv_dev.bronze TO `Desarrolladores`;

-- Analistas NO acceden a bronze (datos crudos no son para consumo)

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_dev.silver (datos limpios y conformados)
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_dev.silver TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_dev.silver TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_dev.silver TO `Ingenieros_Datos`;

GRANT USE SCHEMA ON SCHEMA ecv_dev.silver TO `Desarrolladores`;
-- Desarrolladores solo leen de silver (no modifican)

-- Analistas NO acceden a silver (van directo a gold)

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_dev.gold (modelo dimensional — consumo analítico)
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_dev.gold TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_dev.gold TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_dev.gold TO `Ingenieros_Datos`;

GRANT USE SCHEMA ON SCHEMA ecv_dev.gold TO `Analistas_BI`;
-- Analistas solo leen (SELECT) de gold

GRANT USE SCHEMA ON SCHEMA ecv_dev.gold TO `Desarrolladores`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_dev.ref (tablas de referencia — DIVIPOLA)
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_dev.ref TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_dev.ref TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_dev.ref TO `Ingenieros_Datos`;

GRANT USE SCHEMA ON SCHEMA ecv_dev.ref TO `Analistas_BI`;

GRANT USE SCHEMA ON SCHEMA ecv_dev.ref TO `Desarrolladores`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_dev.audit (tablas de auditoría del pipeline)
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_dev.audit TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_dev.audit TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_dev.audit TO `Ingenieros_Datos`;

-- Solo Arquitectos e Ingenieros ven auditoría

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 6. Acceso a schemas — ecv_prod

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_prod.bronze
-- Solo Ingenieros escriben en prod (vía pipeline automatizado)
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_prod.bronze TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_prod.bronze TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_prod.bronze TO `Ingenieros_Datos`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_prod.silver
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_prod.silver TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_prod.silver TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_prod.silver TO `Ingenieros_Datos`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_prod.gold — Aquí consumen los Analistas
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_prod.gold TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_prod.gold TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_prod.gold TO `Ingenieros_Datos`;

GRANT USE SCHEMA ON SCHEMA ecv_prod.gold TO `Analistas_BI`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_prod.ref
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_prod.ref TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_prod.ref TO `Ingenieros_Datos`;

GRANT USE SCHEMA ON SCHEMA ecv_prod.ref TO `Analistas_BI`;

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Schema: ecv_prod.audit
-- ═══════════════════════════════════════════════════════════════
GRANT USE SCHEMA ON SCHEMA ecv_prod.audit TO `Ingenieros_Datos`;
GRANT CREATE TABLE ON SCHEMA ecv_prod.audit TO `Ingenieros_Datos`;
GRANT MODIFY ON SCHEMA ecv_prod.audit TO `Ingenieros_Datos`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 7. Acceso a tablas Bronze (ecv_dev)

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Tablas Bronze — Ingenieros tienen CRUD, Desarrolladores solo SELECT
-- ═══════════════════════════════════════════════════════════════

-- Ingenieros: acceso completo a bronze
GRANT SELECT, MODIFY ON TABLE ecv_dev.bronze.caract_comp_hogar TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.bronze.datos_vivienda TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.bronze.educacion TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.bronze.divipola_departamentos TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.bronze.divipola_municipios_excel TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.bronze.divipola_municipios_mysql TO `Ingenieros_Datos`;

-- Desarrolladores: solo lectura en bronze
GRANT SELECT ON TABLE ecv_dev.bronze.caract_comp_hogar TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.bronze.datos_vivienda TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.bronze.educacion TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.bronze.divipola_departamentos TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.bronze.divipola_municipios_excel TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.bronze.divipola_municipios_mysql TO `Desarrolladores`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 8. Acceso a tablas Silver (ecv_dev)

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Tablas Silver — Ingenieros CRUD, Desarrolladores SELECT
-- ═══════════════════════════════════════════════════════════════

-- Ingenieros: acceso completo a silver
GRANT SELECT, MODIFY ON TABLE ecv_dev.silver.vivienda TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.silver.hogar TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.silver.educacion TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.silver.persona TO `Ingenieros_Datos`;

-- Desarrolladores: solo lectura en silver
GRANT SELECT ON TABLE ecv_dev.silver.vivienda TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.silver.hogar TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.silver.educacion TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.silver.persona TO `Desarrolladores`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 9. Acceso a tablas Gold (ecv_dev)

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Tablas Gold — Ingenieros CRUD, Analistas y Desarrolladores SELECT
-- ═══════════════════════════════════════════════════════════════

-- Ingenieros: acceso completo a gold
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.dim_persona TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.dim_vivienda TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.dim_educacion TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.dim_ubicacion TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.dim_tiempo TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.fac_ecv TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.kpi_becas_anuales TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_dev.gold.kpi_condiciones_vida_region TO `Ingenieros_Datos`;

-- Analistas: solo lectura en gold (para dashboards y reportes)
GRANT SELECT ON TABLE ecv_dev.gold.dim_persona TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_vivienda TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_educacion TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_ubicacion TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_tiempo TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.gold.fac_ecv TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.gold.kpi_becas_anuales TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.gold.kpi_condiciones_vida_region TO `Analistas_BI`;

-- Desarrolladores: solo lectura en gold
GRANT SELECT ON TABLE ecv_dev.gold.dim_persona TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_vivienda TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_educacion TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_ubicacion TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.gold.dim_tiempo TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.gold.fac_ecv TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.gold.kpi_becas_anuales TO `Desarrolladores`;
GRANT SELECT ON TABLE ecv_dev.gold.kpi_condiciones_vida_region TO `Desarrolladores`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 10. Acceso a tablas de Referencia (ecv_dev)

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Tabla ref.divipola — Catálogo geográfico de Colombia
-- Todos los roles pueden leer, solo Ingenieros pueden modificar
-- ═══════════════════════════════════════════════════════════════
GRANT SELECT, MODIFY ON TABLE ecv_dev.ref.divipola TO `Ingenieros_Datos`;
GRANT SELECT ON TABLE ecv_dev.ref.divipola TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_dev.ref.divipola TO `Desarrolladores`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 11. Acceso a tablas de Auditoría (ecv_dev)

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Tablas audit — Solo Ingenieros y Arquitectos
-- Contienen logs de ejecución del pipeline (información sensible)
-- ═══════════════════════════════════════════════════════════════
GRANT SELECT, MODIFY ON TABLE ecv_dev.audit.pipeline_runs TO `Ingenieros_Datos`;
GRANT SELECT ON TABLE ecv_dev.audit.pipeline_runs TO `Arquitectos_Datos`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 12. Acceso a tablas Gold (ecv_prod) — Analistas consumen de aquí

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Tablas Gold PROD — Analistas solo SELECT para Power BI
-- Los dashboards de Power BI conectan a ecv_prod.gold.*
-- ═══════════════════════════════════════════════════════════════

-- Ingenieros: acceso completo (pipeline automatizado escribe aquí)
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.dim_persona TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.dim_vivienda TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.dim_educacion TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.dim_ubicacion TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.dim_tiempo TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.fac_ecv TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.kpi_becas_anuales TO `Ingenieros_Datos`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.gold.kpi_condiciones_vida_region TO `Ingenieros_Datos`;

-- Analistas: solo lectura (para dashboards de producción)
GRANT SELECT ON TABLE ecv_prod.gold.dim_persona TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_prod.gold.dim_vivienda TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_prod.gold.dim_educacion TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_prod.gold.dim_ubicacion TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_prod.gold.dim_tiempo TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_prod.gold.fac_ecv TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_prod.gold.kpi_becas_anuales TO `Analistas_BI`;
GRANT SELECT ON TABLE ecv_prod.gold.kpi_condiciones_vida_region TO `Analistas_BI`;

-- Referencia prod
GRANT SELECT ON TABLE ecv_prod.ref.divipola TO `Analistas_BI`;
GRANT SELECT, MODIFY ON TABLE ecv_prod.ref.divipola TO `Ingenieros_Datos`;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 13. Verificar grants aplicados

-- COMMAND ----------

-- ═══════════════════════════════════════════════════════════════
-- Verificaciones — ejecutar para confirmar que los grants se aplicaron
-- ═══════════════════════════════════════════════════════════════

-- Verificar acceso al catálogo
SHOW GRANTS ON CATALOG ecv_dev;

-- COMMAND ----------

SHOW GRANTS ON CATALOG ecv_prod;

-- COMMAND ----------

-- Verificar acceso a schemas
SHOW GRANTS ON SCHEMA ecv_dev.bronze;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA ecv_dev.gold;

-- COMMAND ----------

SHOW GRANTS ON SCHEMA ecv_prod.gold;

-- COMMAND ----------

-- Verificar acceso a tablas específicas
SHOW GRANTS ON TABLE ecv_dev.gold.fac_ecv;

-- COMMAND ----------

SHOW GRANTS ON TABLE ecv_prod.gold.fac_ecv;

-- COMMAND ----------

SHOW GRANTS ON TABLE ecv_dev.ref.divipola;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 14. Resumen de permisos por rol
-- MAGIC
-- MAGIC | Recurso | Arquitectos | Ingenieros | Analistas BI | Desarrolladores |
-- MAGIC |---------|-------------|------------|--------------|-----------------|
-- MAGIC | **ecv_dev (catálogo)** | ALL PRIVILEGES | USE CATALOG | USE CATALOG | USE CATALOG |
-- MAGIC | **ecv_prod (catálogo)** | ALL PRIVILEGES | USE CATALOG | USE CATALOG | ❌ Sin acceso |
-- MAGIC | **bronze (schema)** | ALL | CREATE + MODIFY + SELECT | ❌ Sin acceso | CREATE + SELECT |
-- MAGIC | **silver (schema)** | ALL | CREATE + MODIFY + SELECT | ❌ Sin acceso | SELECT |
-- MAGIC | **gold (schema)** | ALL | CREATE + MODIFY + SELECT | SELECT | SELECT |
-- MAGIC | **ref (schema)** | ALL | MODIFY + SELECT | SELECT | SELECT |
-- MAGIC | **audit (schema)** | ALL | MODIFY + SELECT | ❌ Sin acceso | ❌ Sin acceso |
-- MAGIC | **gold prod (tablas)** | ALL | MODIFY + SELECT | SELECT | ❌ Sin acceso |
-- MAGIC
-- MAGIC ### Principios aplicados
-- MAGIC - **Mínimo privilegio**: cada rol solo tiene los permisos necesarios para su función
-- MAGIC - **Separación de ambientes**: Desarrolladores NO acceden a producción
-- MAGIC - **Analistas aislados de datos crudos**: solo ven Gold (datos listos para consumo)
-- MAGIC - **Auditoría restringida**: solo Ingenieros y Arquitectos ven logs del pipeline

