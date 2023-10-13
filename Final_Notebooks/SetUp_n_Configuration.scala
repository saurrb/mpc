// Databricks notebook source
dbutils.widgets.dropdown("environment", "dev", Seq("dev","prd"), "Pick Environment")
// COMMAND ----------
val env = dbutils.widgets.get("environment")
// COMMAND ----------
dbutils.secrets.listScopes
// COMMAND ----------
display(dbutils.fs.mounts)
// COMMAND ----------
// dbutils.fs.unmount("/mnt/local")
// dbutils.fs.unmount("/mnt/dma")
// COMMAND ----------
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" ->dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "clientID"),
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "SPSecret"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/db05faca-c82a-4b9d-b9c5-0f64b6755421/oauth2/token")
// COMMAND ----------
dbutils.fs.mount(
   //source = "abfss://mpcreportingprodadlsfs@mpcreportingprodadls.dfs.core.windows.net/",
   source = s"abfss://mpcreporting${env}dlfs@mpcreporting${env}sa.dfs.core.windows.net/",
   mountPoint = "/mnt/local",
   extraConfigs = configs)
// COMMAND ----------
dbutils.fs.mount(
   source = s"abfss://mpcreporting${env}dlctr@dmadp${env}dls.dfs.core.windows.net/",
   mountPoint = "/mnt/dma",
   extraConfigs = configs)
// COMMAND ----------
// MAGIC %sql
// MAGIC create database if not exists mpc_metrics;
// MAGIC create database if not exists mpc;
// MAGIC --detailtable
// MAGIC --summarytable
// MAGIC --referencetable
// MAGIC --data_load
// COMMAND ----------
val env_nm = if(env=="dev") "dev" else if(env=="prd") "prod" else ""
// COMMAND ----------
import java.util.Properties
import java.sql.{DriverManager, Timestamp, Date}
import org.apache.spark.sql.types._
val dwServer = s"mpcreporting-${env_nm}-sql-server.database.windows.net"
val dwDatabase = s"mpcreporting${env_nm}db"
val dwJdbcPort = "1433"
val synapseuser = dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "adminusername")
val synapsepass = dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "adminpassword")
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
// spark.conf.set("spark.databricks.sqldw.writeSemantics", "polybase")
// spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
// Set up the Blob storage account access key in the spark conf
//Generate Synapse JDBC URL
val SQlDWUrl = s"jdbc:sqlserver://${dwServer}:${dwJdbcPort};database=${dwDatabase};user=${synapseuser};password=${synapsepass};"
// Create JDBC Connection to Synapse
 val connectionProperties = new Properties()
val Connection = DriverManager.getConnection(SQlDWUrl)
val Statement = Connection.createStatement()
// COMMAND ----------
val ODSTableQuery = s"(SELECT * FROM INFORMATION_SCHEMA.TABLES) TABLES"
var ODSTable = spark.read.jdbc(url=SQlDWUrl, table=ODSTableQuery, properties=connectionProperties)
display(ODSTable)
// COMMAND ----------
val result = s"SELECT count(*) as Count FROM INFORMATION_SCHEMA.TABLES"
val rsCount = Statement.executeQuery(result)
rsCount.next
val Difference = rsCount.getInt("Count")
// COMMAND ----------
// MAGIC %fs ls /mnt/dma