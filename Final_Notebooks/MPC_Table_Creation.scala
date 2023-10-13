// Databricks notebook source
// val tableList = dbutils.fs.ls("/mnt/dma").toDF.select("name").map(a=>a.getString(0).replace("/", "")).collect.toList
// spark.sql(s"create database if not exists MPC")
// for (table <- tableList) {
//   println(table)
//   spark.sql(s"create table if not exists MPC.${table} using DELTA location '/mnt/dma/${table}'")
// }
// spark.sql(s"create table if not exists MPC.DATA_LOAD using DELTA location '/mnt/local/Metrics_Table'")
// COMMAND ----------
// MAGIC %sql CREATE DATABASE IF NOT EXISTS MPC
// COMMAND ----------
spark.sql("""CREATE TABLE IF NOT EXISTS MPC.SummaryTable
(
	SRC_LOB            STRING,
    DT_MSG_SND         DATE,
    NBR_OF_MSG_ATMPT   INT,
    NBR_OF_MSG_WTH_NPI INT,
    NBR_OF_PROV_RCH    INT,
    NBR_OF_MSG_SND     INT,
    SUC_RT             DECIMAL(22,2),
    AVG_TM_TO_CNFM     DECIMAL(22,2),
    WINDOW_ID          STRING
) USING DELTA
LOCATION '/mnt/dma/SummaryTable'""")
spark.sql(s"ALTER TABLE MPC.SummaryTable  SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.isolationLevel' = 'WriteSerializable')")
// COMMAND ----------
spark.sql("""CREATE TABLE IF NOT EXISTS MPC.DetailTable
(
	PROV_CMNCT_DTL_ID   STRING,
    FL_REF_NBR          INT,
    PGM_DRG_NM          STRING,
    PROV_FST_NM         STRING,
    PROV_LST_NM         STRING,
    ADR1                STRING,
    ADR2                STRING,
    CTY                 STRING,
    ST                  STRING,
    ZIPCD               STRING,
    PRSCr_NPI           INT,
    NPI_MTCH_STS        STRING,
    PROV_DIR_ADR        STRING,
    DIR_STS             STRING,
    FAIL_RSN            STRING,
    DT_MSG_SND          TIMESTAMP,
    DT_ACPT             TIMESTAMP,
    FAX_NUM             LONG,
    Fax_STS             STRING,
    Fax_ERR_MSG         STRING,
    NBR_Fax_PG          INT,
    Fax_DT_MSG_SND      TIMESTAMP,
    FAX_DT_ACPT         TIMESTAMP,
    OVALL_STS           STRING,
    SRC_LOB             STRING,
    FL_RECV_DT          DATE,
    WINDOW_ID           STRING
) USING DELTA
LOCATION '/mnt/dma/DetailTable'""")
spark.sql(s"ALTER TABLE MPC.DetailTable  SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.isolationLevel' = 'WriteSerializable')")
// COMMAND ----------
// MAGIC %sql
// MAGIC Create table if not exists MPC.DATA_LOAD (
// MAGIC   TableName STRING NOT NULL,
// MAGIC   WindowID STRING NOT NULL,
// MAGIC   source_count BIGINT,
// MAGIC   delta_count BIGINT,
// MAGIC   synapse_count BIGINT,
// MAGIC   count_validation STRING,
// MAGIC   no_columns BIGINT,
// MAGIC   window_inserts BIGINT,
// MAGIC   start_time TIMESTAMP,
// MAGIC   end_time TIMESTAMP,
// MAGIC   elapsed BIGINT,
// MAGIC   src_to_delta BIGINT,
// MAGIC   src_to_synapse BIGINT,
// MAGIC   restore_version BIGINT
// MAGIC )
// MAGIC USING DELTA
// MAGIC PARTITIONED BY (TableName)
// MAGIC LOCATION '/mnt/local/Metrics_Table'
// COMMAND ----------
spark.sql("""CREATE TABLE IF NOT EXISTS MPC.ReferenceTable
(
	SRC_LOB          STRING,
    BUS_OWNR_NM      STRING,
    BUS_OWNR_EMAIL   STRING,
    NM_OF_LOB        STRING,
    TECH_OWNR_NM     STRING,
    TECH_OWNR_EMAIL  STRING,
    RMRK             STRING
) USING DELTA
LOCATION '/mnt/dma/ReferenceTable'""")
spark.sql(s"ALTER TABLE MPC.ReferenceTable  SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.isolationLevel' = 'WriteSerializable')")