// Databricks notebook source
val secret = dbutils.secrets.get(scope = "sql-crdentials", key = "sqlpassword")

print(secret)

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config = Config(Map(
  "url"            -> "analyticday3.database.windows.net",
  "databaseName"   -> "analtyicday3",
  "dbTable"        -> "dbo.Customers",
  "user"           -> "dejosh",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config)

collection.printSchema
collection.createOrReplaceTempView("customers")

    
    

// COMMAND ----------

// MAGIC 
// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM customers

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._

val config1 = Config(Map(
  "url"            -> "analyticday3.database.windows.net",
  "databaseName"   -> "analtyicday3",
  "dbTable"        -> "dbo.Locations",
  "user"           -> "dejosh",
  "password"       -> secret,
  "connectTimeout" -> "5", 
  "queryTimeout"   -> "5"  
))

val collection = spark.read.sqlDB(config1)

collection.printSchema
collection.createOrReplaceTempView("locations")

    
    

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT * FROM Locations

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName), City, State, Country
// MAGIC From customers C
// MAGIC INNER JOIN locations L ON C.LocationId=L.LocationId
// MAGIC WHERE L.State="AP"

// COMMAND ----------

/*val dlsSecret2 = dbutils.secrets.get(scope = "training-scope", key = "dlsaccesskey")
val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "00759323-3305-42ba-984a-477cb51c7edf",
  "fs.azure.account.oauth2.client.secret" -> dlsSecret2,
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

dbutils.fs.mount(
  source = "abfss://data@analyticday3.dfs.core.windows.net/",
  mountPoint = "/mnt/dlsdata",
  extraConfigs = configs)*/


// COMMAND ----------

// MAGIC     %fs
// MAGIC 
// MAGIC ls /mnt/dlsdata/SalesFiles/

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val fileNames = "/mnt/dlsdata/SalesFiles/*.csv"
val schema = StructType(
Array(
StructField("SaleId",IntegerType,true),
StructField("SaleDate",IntegerType,true),
StructField("CustomerId",DoubleType,true),
StructField("EmployeeId",DoubleType,true),
StructField("StoreId",DoubleType,true),
StructField("ProductId",DoubleType,true),
StructField("NoOfUnits",DoubleType,true),
StructField("SaleAmount",DoubleType,true),
StructField("SalesReasonId",DoubleType,true),
StructField("ProductCost",DoubleType,true)
)
)

val data = spark.read.option("inferSchema",false).option("header","true").option("sep",",").schema(schema).csv(fileNames)

data.printSchema
data.createOrReplaceTempView("FactSales")


// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TEMP VIEW ProcessedResults
// MAGIC AS
// MAGIC SELECT C.CustomerId, CONCAT(FName, " ", MName, " ", LName) AS FullName,
// MAGIC   C.CreditLimit, C.ActiveStatus, L.City, L.State, L.Country,
// MAGIC   S.SaleAmount, S.NoOfUnits
// MAGIC FROM Customers C
// MAGIC INNER JOIN Locations L ON C.LocationId = L.LocationId
// MAGIC INNER JOIN factsales S on S.CustomerId = C.CustomerId
// MAGIC WHERE L.State in ( "AP", "TN" )

// COMMAND ----------

 val results = spark.sql("SELECT * FROM ProcessedResults")

results.printSchema
results.write.mode("append").parquet("/mnt/dlsdata/parque/sales")

// COMMAND ----------


val processedSales = spark.read.format("parquet").option("header", "true").option("inferschema", "true").load("/mnt/dlsdata/parque/sales")
processedSales.printSchema
processedSales.show(100, false)

// COMMAND ----------

// DBTITLE 1,DW and Poly-base Configuration for Bulk Updates
val blobStorage = "analtyticday3typical.blob.core.windows.net"
	val blobContainer = "temp-data"
	val blobAccessKey =  "uEv/FvbkPgr1HB0cctA6P31vd3zTMGNGmfaVuxlPfa2vPneHoI0KfdQgV7oj9lXDWSae9pM5prbHiKiq7wnnMg=="
	val tempDir = "wasbs://" + blobContainer + "@" + blobStorage +"/tempDirs"


val acntInfo = "fs.azure.account.key."+ blobStorage
sc.hadoopConfiguration.set(acntInfo, blobAccessKey)


// COMMAND ----------

	val dwDatabase = "analyticdw"
	val dwServer = "analyticday3.database.windows.net"
	val dwUser = "dejosh"
	val dwPass = "Dngirish@123"
	val dwJdbcPort =  "1433"
	val sqlDwUrlSmall = "jdbc:sqlserver://" + dwServer + ":" + dwJdbcPort + ";database=" + dwDatabase + ";user=" + dwUser+";password=" + dwPass

	spark.conf.set(
		"spark.sql.parquet.writeLegacyFormat",
		"true")

// COMMAND ----------

	results.write
		.format("com.databricks.spark.sqldw")
		.option("url", sqlDwUrlSmall) 
		.option("dbtable", "ProcessedResults")
		.option("forward_spark_azure_storage_credentials","True")
		.option("tempdir", tempDir)
		.mode("overwrite")
		.save()
        