// Databricks notebook source
import java.util.Properties
import java.sql.{DriverManager, Timestamp, Date}
import java.time.format.DateTimeFormatter
import java.util.Properties
import java.sql.Date
import spark.implicits._
import spark.sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.collection.immutable.Map
import org.apache.spark.sql.functions.input_file_name
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
// COMMAND ----------
dbutils.widgets.dropdown("environment", "dev", Seq("dev", "prod"), "Pick Environment")
val env=dbutils.widgets.get("environment")
// COMMAND ----------
val dwServer = s"mpcreporting-${env}-sql-server.database.windows.net"
val dwDatabase = s"mpcreporting${env}db"
val dwJdbcPort = "1433"
// val synapsepass = dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "sqladminpass")
// val synapseuser = dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "sqladminuser")
val synapsepass = dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "adminpassword")
val synapseuser = dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "adminusername")
val dwJdbcExtraOptions = "encrypt=true;trustServerCertificate=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
// val storageAccountKey = dbutils.secrets.get(scope = "mpcreporting-dbscope", key = "SAAccessKey")
// spark.conf.set("fs.azure.account.key.mpcreporting${env}adls.dfs.core.windows.net", storageAccountKey)
// val TempDir = "abfss://mpcreporting${env}adlsfs@mpcreporting${env}adls.dfs.core.windows.net/temp/"
val SQlDWUrl = s"jdbc:sqlserver://${dwServer}:${dwJdbcPort};database=${dwDatabase};user=${synapseuser};password=${synapsepass};"
val connectionProperties = new Properties()
val Connection = DriverManager.getConnection(SQlDWUrl)
val Statement = Connection.createStatement()
// COMMAND ----------
Statement.execute(s"ALTER TABLE PROV_COM_DATA.PROV_CMNCT_DTL add FAX_DT_ACPT datetime")
// COMMAND ----------
Statement.execute(s"ALTER TABLE PROV_COM_DATA.PROV_CMNCT_DTL add Fax_DT_MSG_SND datetime")
// COMMAND ----------
Statement.execute(s"ALTER TABLE PROV_COM_DATA.PROV_CMNCT_DTL add FAX_NUM bigint")
// COMMAND ----------
val ODSTableQuery = "(SELECT * FROM [PROV_COM_DATA].[PROV_CMNCT_DTL]) [PROV_CMNCT_DTL]"
var ODSTable = spark.read.jdbc(url=SQlDWUrl, table=ODSTableQuery, properties=connectionProperties)
// COMMAND ----------
ODSTable.write.mode("append").format("delta").save("/mnt/dma/DetailTable")