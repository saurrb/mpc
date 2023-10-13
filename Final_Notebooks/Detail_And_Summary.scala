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
val dateFormat = "yyyyMMddHHmm"
val WindowID = spark.range(1).select(date_format(current_timestamp, dateFormat)).as[(String)].first
val fileList_Df = dbutils.fs.ls("dbfs:/mnt/local/ECG_Inbound/").toDF
val fileList = fileList_Df.select("name").filter($"name".endsWith("report.csv"))                       //Historical
                .collect().map(_(0)).toList
// COMMAND ----------
if(fileList.size <= 0) dbutils.notebook.exit("No new file found")
// COMMAND ----------
val schema = new StructType()
      .add("IDENTIFIER", StringType, true)
      .add("ROW_NUMBER", IntegerType,true)
      .add("PROGRAM", StringType,true)
      .add("PROVIDER_FIRST_NAME", StringType,true)
      .add("PROVIDER_LAST_NAME", StringType,true)
      .add("ADDRESS1", StringType,true)
      .add("ADDRESS2", StringType,true)
      .add("CITY", StringType,true)
      .add("STATE", StringType,true)
      .add("ZIPCODE", IntegerType,true)
      .add("PRESCRIBER_NPI", IntegerType,true)
      .add("NPI Match Status", StringType,true)
      .add("Provider_Directaddress", StringType,true)
      .add("Direct Status", StringType,true)
      .add("Failure Reason", StringType,true)
      .add("Date Message Sent", StringType,true)
      .add("Date Failed/Confirmed", StringType,true)
      .add("FAX_NUMBER", LongType,true)
      .add("FAX_STATUS", StringType,true)
      .add("FAX_ERROR_MESSAGES", StringType,true)
      .add("NUMBER_FAX_PAGES", IntegerType,true)
      .add("FAX_Date Message Sent", StringType,true)
      .add("FAX_Date_Failed/Confirmed", StringType,true)
      .add("OVERALL_STATUS", StringType,true)
// COMMAND ----------
var csvDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        for(file <- fileList){
                    var tempSourceDf = spark.read.options(Map("header"->"true","dateFormat"->"MM/dd/yyyy hh:mm:ss AM/PM"))
                                        .csv(s"dbfs:/mnt/local/ECG_Inbound/${file}").drop("_c17")
                                        .withColumn("SRC_LOB",  split(input_file_name,"_").getItem(2))
                                        .withColumn("Sent_Date",  to_date(reverse(split(input_file_name,"_")).getItem(2) , "MMddyyyy")  )
                    csvDF = csvDF.unionByName(tempSourceDf, true)
        }
display(csvDF)
// COMMAND ----------
try{
  csvDF = csvDF.select("IDENTIFIER", "ROW_NUMBER", "PROGRAM", "PROVIDER_FIRST_NAME", "PROVIDER_LAST_NAME", "ADDRESS1", "ADDRESS2", "CITY", "STATE", "ZIPCODE", "PRESCRIBER_NPI", "NPI Match Status", "Provider_Directaddress", "Direct Status", "Failure Reason", "Date Message Sent", "Date Failed/Confirmed","Fax_NUMBER","FAX_STATUS","FAX_ERROR_MESSAGES","NUMBER_FAX_PAGES","FAX_Date Message Sent","FAX_Date_Failed/Confirmed","OVERALL_STATUS", "SRC_LOB", "Sent_Date")
}
catch{
  case error: Exception => {
    var exitValue = "Column not found"
    dbutils.notebook.exit(exitValue)
  }
}
// COMMAND ----------
val sourceNames = Seq("IDENTIFIER","ROW_NUMBER","PROGRAM","PROVIDER_FIRST_NAME","PROVIDER_LAST_NAME","ADDRESS1","ADDRESS2","CITY","STATE","ZIPCODE","PRESCRIBER_NPI","NPI Match Status","Provider_Directaddress","Direct Status","Failure Reason","Date Message Sent","Date Failed/Confirmed","Fax_NUMBER","FAX_STATUS",
 "FAX_ERROR_MESSAGES","NUMBER_FAX_PAGES","FAX_Date Message Sent","FAX_Date_Failed/Confirmed","OVERALL_STATUS","SRC_LOB","Sent_Date")
val targetNames = Seq("PROV_CMNCT_DTL_ID","FL_REF_NBR","PGM_DRG_NM","PROV_FST_NM","PROV_LST_NM","ADR1","ADR2","CTY","ST","ZIPCD","PRSCr_NPI","NPI_MTCH_STS","PROV_DIR_ADR","DIR_STS",
                      "FAIL_RSN","DT_MSG_SND","DT_ACPT","FAX_NUM","Fax_STS","Fax_ERR_MSG","NBR_Fax_PG","Fax_DT_MSG_SND","FAX_DT_ACPT","OVALL_STS","SRC_LOB","FL_RECV_DT")
val detail_sourceDf = csvDF
                    .withColumn("Date Message Sent", when($"Date Message Sent".contains("AM") or $"Date Message Sent".contains("PM"),
                                                          to_timestamp(csvDF("Date Message Sent"), "MM/dd/yyyy hh:mm:ss aa"))
                                .otherwise(to_timestamp(csvDF("Date Message Sent"), "MM/dd/yyyy HH:mm:ss")))
                    .withColumn("Date Failed/Confirmed", when($"Date Failed/Confirmed".contains("AM") or $"Date Failed/Confirmed".contains("PM"),
                                                          to_timestamp(csvDF("Date Failed/Confirmed"), "MM/dd/yyyy hh:mm:ss aa"))
                                .otherwise(to_timestamp(csvDF("Date Failed/Confirmed"), "MM/dd/yyyy HH:mm:ss")))
                    .withColumn("ZIPCODE", $"ZIPCODE".cast(StringType))
                    .withColumn("PRESCRIBER_NPI",($"PRESCRIBER_NPI").cast(IntegerType))
                    .withColumn("ROW_NUMBER",$"ROW_NUMBER".cast(IntegerType))
                    .withColumn("Fax_NUMBER",$"Fax_NUMBER".cast(LongType))
                    .withColumn("NUMBER_FAX_PAGES",$"NUMBER_FAX_PAGES".cast(IntegerType))
                    .withColumn("OverallStatus",when(csvDF("Direct Status") === "Confirmed","Successful").otherwise("Failed"))
                    .withColumn("FAX_Date Message Sent", when($"FAX_Date Message Sent".contains("AM") or $"FAX_Date Message Sent".contains("PM"),
                                                          to_timestamp(csvDF("FAX_Date Message Sent"), "MM/dd/yyyy hh:mm:ss aa"))
                                .otherwise(to_timestamp(csvDF("FAX_Date Message Sent"), "MM/dd/yyyy HH:mm:ss")))
                    .withColumn("FAX_Date_Failed/Confirmed", when($"FAX_Date_Failed/Confirmed".contains("AM") or $"FAX_Date_Failed/Confirmed".contains("PM"),
                                                          to_timestamp(csvDF("FAX_Date_Failed/Confirmed"), "MM/dd/yyyy hh:mm:ss aa"))
                                .otherwise(to_timestamp(csvDF("FAX_Date_Failed/Confirmed"), "MM/dd/yyyy HH:mm:ss")))
                    .select(sourceNames.head, sourceNames.tail:_*)
                    .toDF(targetNames : _*)
                    .withColumn("WINDOW_ID", lit(WindowID))
                    .na.fill("",Seq("Fax_STS","Fax_ERR_MSG","OVALL_STS"))
// COMMAND ----------
display(detail_sourceDf)
// COMMAND ----------
detail_sourceDf.count
// COMMAND ----------
var start_time = new Timestamp(System.currentTimeMillis())
detail_sourceDf.write.mode("append").format("delta").save("/mnt/dma/DetailTable")
// COMMAND ----------
val checkPoint = new Timestamp(System.currentTimeMillis())
val ODSTableQuery = "(SELECT TOP 1  * FROM [PROV_COM_DATA].[PROV_CMNCT_DTL]) [PROV_CMNCT_DTL]"
var ODSTable = spark.read.jdbc(url=SQlDWUrl, table=ODSTableQuery, properties=connectionProperties)
var detailTarget = detail_sourceDf.select(ODSTable.columns.head, ODSTable.columns.tail: _*)
// ODSTable.columns
// COMMAND ----------
detailTarget.write.mode("append").jdbc(SQlDWUrl,"[PROV_COM_DATA].[PROV_CMNCT_DTL]",connectionProperties)
var end_time = new Timestamp(System.currentTimeMillis())
// COMMAND ----------
// var mpctable = spark.read.jdbc(SQlDWUrl, "[PROV_COM_DATA].[PROV_CMNCT_DTL]", connectionProperties)
// // mpctable = mpctable.where($"WINDOW_ID"==="202108201423")
// display(mpctable)
// COMMAND ----------
var TableName = "DetailTable"
var src_to_delta = (checkPoint.getTime - start_time.getTime)/(60 * 1000)
var src_to_synapse = (end_time.getTime - checkPoint.getTime)/(60 * 1000) // In minutes
var elapsed = (end_time.getTime - start_time.getTime)/(60 * 1000) // In minutes
var source_count = csvDF.count
var delta_count = spark.table("MPC.DetailTable").count
var sqlStmt =  s"(SELECT COUNT_BIG(*) as count FROM [PROV_COM_DATA].[PROV_CMNCT_DTL]) [PROV_CMNCT_DTL]"
var synapse_count = spark.read.jdbc(url=SQlDWUrl, table=sqlStmt, properties=connectionProperties).first.getAs[Long]("count")
var no_columns = spark.table("MPC.DetailTable").columns.size
var window_inserts = detail_sourceDf.count
var count_validation = "PASSED"
if(delta_count != synapse_count) count_validation = "FAILED"
var restore_version = spark.sql(s"describe history MPC.DetailTable").select("version").limit(1).as[(Long)].first
var InsertQuery = s"INSERT INTO MPC.DATA_LOAD VALUES ('${TableName}', '${WindowID}', '${source_count}','${delta_count}', '${synapse_count}', '${count_validation}', '${no_columns}', '${window_inserts}', '${start_time}', '${end_time}', '${elapsed}', '${src_to_delta}', '${src_to_synapse}','${restore_version}')"
spark.sql(InsertQuery)
    count_validation
// COMMAND ----------
// Sent_Date taking from DateMessageSent
// Handle case where DateMessageSent field is null -> Keep Sent_Date as original - the value extracted from file name
val csvDF2 = csvDF.withColumn("Sent_Date_Including_Null", to_date(split(csvDF("Date Message Sent")," ").getItem(0) , "MM/dd/yyyy"))
                  .withColumn("Sent_Date", when($"Sent_Date_Including_Null".isNull, $"Sent_Date").otherwise($"Sent_Date_Including_Null"))
                  .drop("Sent_Date_Including_Null")
// display(csvDF2)
// COMMAND ----------
display(csvDF2)
// COMMAND ----------
val totalCaseDF = csvDF2//.where($"Date Message Sent".isNotNull)
                      .withColumnRenamed("SRC_LOB","SRC_LOB_temp")
                      .withColumn("Sent_Date_temp", $"Sent_Date")
                      .drop("Sent_Date")
                      .groupBy("SRC_LOB_temp", "Sent_Date_temp")
                      .agg(countDistinct("Provider_Directaddress").as("NBR_OF_PROV_RCH"),count("SRC_LOB_temp").as("NBR_OF_MSG_ATMPT"))
val phaseDF = csvDF2.where(($"NPI Match Status" === "Found")
                          and $"Provider_Directaddress".isNotNull and $"Date Message Sent".isNotNull) //and $"Program".isNotNull
                      .withColumn("Sent_Date", to_date(split(csvDF2("Date Message Sent")," ").getItem(0) , "MM/dd/yyyy"))
                      .select("SRC_LOB", "Sent_Date", "Date Message Sent", "Date Failed/Confirmed", "Direct Status", "Failure Reason")
                      .withColumn("Date Message Sent", when($"Date Message Sent".contains("AM") or $"Date Message Sent".contains("PM"),
                                                          to_timestamp(csvDF2("Date Message Sent"), "MM/dd/yyyy hh:mm:ss aa"))
                                .otherwise(to_timestamp(csvDF2("Date Message Sent"), "MM/dd/yyyy HH:mm:ss")))
                      .withColumn("Date Failed/Confirmed", when($"Date Failed/Confirmed".contains("AM") or $"Date Failed/Confirmed".contains("PM"),
                                                          to_timestamp(csvDF2("Date Failed/Confirmed"), "MM/dd/yyyy hh:mm:ss aa"))
                                .otherwise(to_timestamp(csvDF2("Date Failed/Confirmed"), "MM/dd/yyyy HH:mm:ss")))
                      .withColumn("DiffInSeconds",abs(col("Date Failed/Confirmed").cast(LongType) - col("Date Message Sent").cast(LongType)))
                      .withColumn("DiffInSeconds",when(col("DiffInSeconds")>0,col("DiffInSeconds")).otherwise(0))
                      .drop("Date Message Sent", "Date Failed/Confirmed")
                      .withColumn("Failed Status", when(col("Direct Status") === "Failed",1))
val sumDF = phaseDF.groupBy("SRC_LOB", "Sent_Date")
                      .agg(sum("DiffInSeconds").as("Total_diff"), count("SRC_LOB").as("Total_count"),
                           sum("Failed Status").as("Failed"))
                      .withColumn("AVG_TM_TO_CNFM", $"Total_diff"/$"Total_count")
                      .withColumn("Failed", when($"Failed".isNull, 0).otherwise($"Failed"))
val tempSourceDF = sumDF
                      .withColumn("NBR_OF_MSG_WTH_NPI",($"Total_count").cast(IntegerType))
                      .withColumn("NBR_OF_MSG_SND",($"Total_count" - $"Failed").cast(IntegerType))
                      .withColumn("SUC_RT",(($"NBR_OF_MSG_SND"/$"NBR_OF_MSG_WTH_NPI")*100).cast(DecimalType(22,2)))
                      .withColumn("AVG_TM_TO_CNFM", $"AVG_TM_TO_CNFM".cast(DecimalType(22,2)))
                      .drop("Total_diff","Total_count","Failed")
val joinDF = tempSourceDF.join(totalCaseDF, tempSourceDF("SRC_LOB")===totalCaseDF("SRC_LOB_temp")
                               and tempSourceDF("Sent_Date")===totalCaseDF("Sent_Date_temp"),"fullouter")
                      .withColumn("SRC_LOB",when($"SRC_LOB".isNull, $"SRC_LOB_temp").otherwise($"SRC_LOB"))
                      .withColumn("Sent_Date",when($"Sent_Date".isNull, $"Sent_Date_temp").otherwise($"Sent_Date"))
                      .drop("SRC_LOB_temp","Sent_Date_temp")
                      .withColumnRenamed("Sent_Date","DT_MSG_SND")
val summary_sourceDf_temp = joinDF.withColumn("NBR_OF_MSG_ATMPT", $"NBR_OF_MSG_ATMPT".cast(IntegerType))
                      .withColumn("NBR_OF_PROV_RCH", $"NBR_OF_PROV_RCH".cast(IntegerType))
                      .where($"SRC_LOB".isNotNull and $"DT_MSG_SND".isNotNull)
                      .select("SRC_LOB", "DT_MSG_SND", "NBR_OF_MSG_ATMPT", "NBR_OF_MSG_WTH_NPI", "NBR_OF_PROV_RCH", "NBR_OF_MSG_SND", "SUC_RT", "AVG_TM_TO_CNFM")
                      .withColumn("WINDOW_ID", lit(WindowID))
// COMMAND ----------
val summary_sourceDf = summary_sourceDf_temp.withColumn("NBR_OF_MSG_WTH_NPI",when($"NBR_OF_MSG_WTH_NPI".isNull, $"NBR_OF_PROV_RCH").otherwise($"NBR_OF_MSG_WTH_NPI"))
                  .withColumn("NBR_OF_MSG_SND",when($"NBR_OF_MSG_SND".isNull, 0).otherwise($"NBR_OF_MSG_SND"))
                  .withColumn("SUC_RT",when($"SUC_RT".isNull, (($"NBR_OF_MSG_WTH_NPI"/$"NBR_OF_MSG_ATMPT")*100).cast(DecimalType(22,2))).otherwise($"SUC_RT"))
                  .withColumn("AVG_TM_TO_CNFM",when($"AVG_TM_TO_CNFM".isNull, 0).otherwise($"AVG_TM_TO_CNFM"))
// COMMAND ----------
display(summary_sourceDf)
// COMMAND ----------
summary_sourceDf.count
// COMMAND ----------
var start_time = new Timestamp(System.currentTimeMillis())
summary_sourceDf.write.mode("append").format("delta").save("/mnt/dma/SummaryTable")
// COMMAND ----------
val checkPoint = new Timestamp(System.currentTimeMillis())
val ODSTableQuery = "(SELECT TOP 1  * FROM [PROV_COM_DATA].[PROV_CMNCT_SUM]) [PROV_CMNCT_SUM]"
var ODSTable = spark.read.jdbc(url=SQlDWUrl, table=ODSTableQuery, properties=connectionProperties)
var summaryTarget = summary_sourceDf.select(ODSTable.columns.head, ODSTable.columns.tail: _*)
// COMMAND ----------
summaryTarget.write.mode("append").jdbc(SQlDWUrl,"[PROV_COM_DATA].[PROV_CMNCT_SUM]",connectionProperties)
var end_time = new Timestamp(System.currentTimeMillis())
// COMMAND ----------
// var mpctable = spark.read.jdbc(SQlDWUrl, "[PROV_COM_DATA].[PROV_CMNCT_SUM]", connectionProperties)
// // mpctable = mpctable.where($"WINDOW_ID"==="202108201537")
// display(mpctable)
// COMMAND ----------
var TableName = "SummaryTable"
var src_to_delta = (checkPoint.getTime - start_time.getTime)/(60 * 1000)
var src_to_synapse = (end_time.getTime - checkPoint.getTime)/(60 * 1000) // In minutes
var elapsed = (end_time.getTime - start_time.getTime)/(60 * 1000) // In minutes
var source_count = csvDF.count
var delta_count = spark.table("MPC.SummaryTable").count
var sqlStmt =  s"(SELECT COUNT_BIG(*) as count FROM [PROV_COM_DATA].[PROV_CMNCT_SUM]) [PROV_CMNCT_SUM]"
var synapse_count = spark.read.jdbc(url=SQlDWUrl, table=sqlStmt, properties=connectionProperties).first.getAs[Long]("count")
var no_columns = spark.table("MPC.SummaryTable").columns.size
var window_inserts = summary_sourceDf.count
var count_validation = "PASSED"
if(delta_count != synapse_count) count_validation = "FAILED"
var restore_version = spark.sql(s"describe history MPC.SummaryTable").select("version").limit(1).as[(Long)].first
var InsertQuery = s"INSERT INTO MPC.DATA_LOAD VALUES ('${TableName}', '${WindowID}', '${source_count}','${delta_count}', '${synapse_count}', '${count_validation}', '${no_columns}', '${window_inserts}', '${start_time}', '${end_time}', '${elapsed}', '${src_to_delta}', '${src_to_synapse}','${restore_version}')"
spark.sql(InsertQuery)
    count_validation
// COMMAND ----------
// COMMAND ----------
// MAGIC %sql select * from mpc.data_load limit 10
// COMMAND ----------
// val ODSTableQuery = "(SELECT * FROM [PROV_COM_DATA].[PROV_CMNCT_DTL]) [PROV_CMNCT_DTL]"
// var ODSTable = spark.read.jdbc(url=SQlDWUrl, table=ODSTableQuery, properties=connectionProperties)
// display(ODSTable)
// COMMAND ----------
// NOTIFICATION ALERTS
// Floor or minimum number of attempts in a day for email to be triggered.
// If there are only three messages attempted and one fails, we don't want an email to be sent.
// If there are at least 10 records in a day, we want to look at match rate or success rate
// If match% is less than 50%
// Source LoB 
// Messages Attempted(total number of rows) 
// Messages with NPI-DM address match(where NPI status is found)  
// Messages Successful (where direct status is success) 
// Match %(col4/col4) 
// Success %(col5/col4)
// COMMAND ----------
val customAlertDF = csvDF2.select("NPI Match Status","Direct Status","SRC_LOB","Sent_Date")
                        .withColumn("total_message",lit(1))
                        .withColumn("NPI_MATCHED_flag",when($"NPI Match Status"==="Found", 1).otherwise(0))
                        .withColumn("Direct_Status_Success_flag",when($"Direct Status"==="Confirmed", 1).otherwise(0))
//                         .drop("NPI Match Status","Direct Status")
val messageDF = customAlertDF.select("SRC_LOB","Sent_Date","total_message","NPI_MATCHED_flag","Direct_Status_Success_flag")
                        .groupBy("SRC_LOB","Sent_Date")
                        .agg(sum("total_message").as("Messages Attempted"),
                             sum("NPI_MATCHED_flag").as("Messages with NPI-DM address match"),
                             sum("Direct_Status_Success_flag").as("Messages Successful"))
                        .withColumn("Match %", (($"Messages with NPI-DM address match"/$"Messages Attempted")*100).cast(DecimalType(5,2)))
                        .withColumn("Success % of NPI-matches", (($"Messages Successful"/$"Messages with NPI-DM address match")*100).cast(DecimalType(5,2)))
                        .filter($"Messages Attempted">10)
                        .filter($"Match %"<50 || $"Success % of NPI-matches"<80)
                        .na.fill(0,Seq("Success % of NPI-matches"))
display(messageDF)
// COMMAND ----------
if(messageDF.count()==0){
  for(file <- fileList){
     dbutils.fs.mv(s"dbfs:/mnt/local/ECG_Inbound/${file}", s"/mnt/local/Archieve_Files/${WindowID}/${file}")
        }
  dbutils.notebook.exit("NoAlerts")
}
// COMMAND ----------
val messageDFMap = messageDF.collect().map(row => {
   Map("SRC_LOB" -> row.get(0),
       "Sent_Date" -> row.get(1),
       "Messages Attempted" -> row.get(2),
       "Messages with NPI-DM address match" -> row.get(3),
       "Messages Successful" -> row.get(4),
       "Match %" -> row.get(5),
       "Success % of NPI-matches" -> row.get(6))
})
val messageDFTable: String =
  messageDFMap.map { map =>
    s"<tr><td align='center'>${map("SRC_LOB")}</td><td align='center'>${map("Sent_Date")}</td><td align='center'>${map("Messages Attempted")}</td><td align='center'>${map("Messages with NPI-DM address match")}</td><td align='center'>${map("Messages Successful")}</td><td align='center'>${map("Match %")}</td><td align='center'>${map("Success % of NPI-matches")}</td></tr>"
  }.mkString(
    "<table border=3><tr><th>SRC_LOB</th><th>Sent_Date</th><th>Messages Attempted</th><th>Messages with NPI-DM address match</th><th>Messages Successful</th><th>Match %</th><th>Success % of NPI-matches</th></tr>",
    "",
    "</table>")
// COMMAND ----------
var alertMessage = "<b>***** This is an automated e-mail message. Please do not reply. *****</b><br></br>"
alertMessage+=  "Team,<br></br>On "
alertMessage+=  (java.time.LocalDate.now).toString
alertMessage+=  " , the DM match rate or success rate was lower than the required cutoff. Please see the data by LoB for the day:<br></br>"
alertMessage+=  messageDFTable + "<br></br>"
alertMessage+= "You can view details for previous days on the <a href='https://app.powerbi.com/groups/me/apps/b382f0dd-5cfe-4506-a2e6-574b459f5bfc/reports/23360fba-2d82-4d4a-93d2-9d4e4a6cdf55/ReportSection'>reporting dashboard</a>.<br></br>"
alertMessage+= "You are being notified since you have been listed as a business owner for Secure Direct Messaging. If you do not wish to receive these alerts anymore, please "
alertMessage+= "<a href='https://forms.office.com/'>opt out using this form by nominating another business owner.</a>"
// COMMAND ----------
val alertLocation = "/mnt/local/alerts/"
val date = DateTimeFormatter.ofPattern("dd_MM_yyyy").format(java.time.LocalDate.now)
// COMMAND ----------
dbutils.fs.put(alertLocation+date+".txt",alertMessage, true)
// COMMAND ----------
// displayHTML(alertMessage)
// COMMAND ----------
for(file <- fileList){
     dbutils.fs.mv(s"dbfs:/mnt/local/ECG_Inbound/${file}", s"/mnt/local/Archieve_Files/${WindowID}/${file}")
        }
// COMMAND ----------
dbutils.notebook.exit(alertMessage)
// COMMAND ----------