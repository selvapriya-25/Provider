// Databricks notebook source
 spark.conf.set("spark.sql.shuffle.partitions", "5001")
      spark.conf.set("spark.sql.broadcastTimeout", "1200")
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "524288000")
     spark.conf.set("serializer", "org.apache.spark.serializer.KryoSerializer")
     // spark.conf.set("spark.kryoserializer.buffer.max.mb", "2048")
     // spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
      spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
//Added below Spark configuration for DLAM
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")

// COMMAND ----------

//package com.molina.providerline
import java.io.FileReader
import java.util.Properties
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import java.sql.Timestamp
import org.apache.hadoop.io.nativeio.NativeIO.Windows
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

// COMMAND ----------

/*object provider_specialty {
  def main(args: Array[String]) {
 
    val sqlContext = SparkSession.builder().getOrCreate()
    sqlContext.sparkContext.setLogLevel("WARN")
    //val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)
 
    try {-infinite dlam 1529*/
 
      //var StateCode = "FL"
      //var FromDate = "2017-01-01"
      //var ToDate = "2017-01-01" //"2017-12-31"
      //var FromDate = args(0)
      //var ToDate = args(1)
      //var StateCode = args(2)
      var FromDate = "2017-01-01"
      var qnxt_database="qnxt_db"  //Infinite DLAM -2842
      var ToDate = spark.sql("select cast(to_date(current_timestamp) as string)").collect().map(_.getString(0)).mkString(",")
      //var FromDate = args(0)
     

// COMMAND ----------

      //var spec = spark.table("qnxt_db.specialty_txn") -removed _txn for infinite dlam 1529
//var spec = spark.table("qnxt_db.specialty")
//var spec = spark.table(qnxt_database+".specialty") //Infinite DLAM -2842
//--Replacing source_state as part_state and adding part_state as ods_part_state for DLAM - 3465 sprint 17
//Adding where clause for unv and unv2 for sprint 19 dlam
/* var spec = spark.sql("""select specialtycode, description, createid, createdate, updateid, lastupdate, state, year, source_state as part_state, job_date, part_state as ods_part_state, part_year from """+qnxt_database+""".specialty where (part_state not in ('NV','AZ','KY','MS','ID','VA','MA'))""")
*/ //removing ods_part_state dlam 3806
//Added IA-Infosys --Added NE Implementation 10/17/23 jahnavi --Added CA infosys
var spec = spark.sql("""select trim(specialtycode) as specialtycode, description, createid, createdate, updateid, lastupdate, state, year, source_state as part_state, job_date, part_year from """+qnxt_database+""".specialty where (trim(part_state) not in ('NV','AZ','KY','MS','ID','VA','MA','IA','NE','CA','CT'))or source_state in('CA')""")
 
/*spec = spec.withColumn("rnk", row_number().over(Window.partitionBy("part_state", "specialtycode").orderBy(desc("lastupdate")))).filter("rnk=1").drop("rnk")
var spec_del = spark.table("qnxt_db.specialty_del")
spec = spec.as("a").join(spec_del.as("b"), col("a.part_state") === col("b.part_state") && col("a.specialtycode") === col("b.specialtycode") && (col("a.createdate") === col("b.createdate") || col("a.lastupdate") === col("b.lastupdate")), "left").select(col("a.*"), col("b.specialtycode").as("b_specialtycode")).filter("b_specialtycode is Null")   - removed _del tables as not available now - infinite dlam 1529 */
 
//var provspec = spark.table("qnxt_db.provspecialty_txn").filter("effdate <= termdate")-removed _txn - infinite dlam 1529
//var provspec = spark.table("qnxt_db.provspecialty").filter("effdate <= termdate")
//var provspec = spark.table(qnxt_database+".provspecialty").filter("effdate <= termdate") //Infinite DLAM -2842
//--Replacing source_state as part_state for DLAM - 3465 sprint 17
/* var provspec = spark.sql("""select provid, specialtycode, spectype, effdate, termdate, lastupdate, updateid, specialtystatus, createid, createdate, state, year, source_state as part_state, job_date, part_state as ods_part_state, part_year from """+qnxt_database+""".provspecialty""").filter("effdate <= termdate")
*/ //removing ods_part_state dlam 3806
var provspec = spark.sql("""select trim(provid) as provid, trim(specialtycode) as specialtycode, spectype, effdate, termdate, lastupdate, updateid, specialtystatus, createid, createdate, state, year, trim(source_state) as part_state, job_date, part_year from """+qnxt_database+""".provspecialty""").filter("effdate <= termdate")
 
provspec = provspec.withColumn("spectype",upper(col("spectype")))
//provspec = provspec.withColumn("provl_rnk", row_number().over(Window.partitionBy("part_state", "provid", "specialtycode").orderBy(desc("lastupdate")))).filter("provl_rnk=1").drop("provl_rnk")- removed - infinite dlam 1529
//var provspec_del = spark.table("qnxt_db.provspecialty_del")
 
//provspec = provspec.as("a").join(provspec_del.as("b"), col("a.part_state") === col("b.part_state") && col("a.provid") === col("b.provid") && (col("a.createdate") === col("b.createdate") || col("a.lastupdate") === col("b.lastupdate")) && col("a.specialtycode") === col("b.specialtycode"), "left").select(col("a.*"), col("b.provid").as("b_provid")).filter("b_provid is Null")
 
//Code added as part of RA project
var spec_join = provspec.as("p").join(spec.as("s"), Seq("specialtycode", "part_state"), "inner").select(col("p.provid"), col("specialtycode"), col("p.part_state"), col("p.spectype"), col("p.lastupdate"), col("description"), col("p.effdate"), col("p.termdate"), col("p.createdate"), col("p.specialtystatus")) // to test updated left to inner
//reverting back to remove ods_part_state dlam 3806
//Adding ods_part_state for sprint 17 DLAM -3465
/*var spec_join = provspec.as("p").join(spec.as("s"), Seq("specialtycode", "part_state")).select(col("p.provid"), col("specialtycode"), col("p.part_state"), col("p.spectype"), col("p.lastupdate"), col("description"), col("p.effdate"), col("p.termdate"), col("p.createdate"), col("p.ods_part_state")) */
 
//var provider = spark.table("qnxt_db.provider_txn") -removed _txn - infinite dlam 1529
//var provider = spark.table("qnxt_db.provider")
//var provider = spark.table(qnxt_database+".provider") //Infinite DLAM -2842
//--Replacing source_state as part_state and adding part_state as ods_part_state for DLAM - 3465 sprint 17
   /*   var provider = spark.sql("""select provid, entityid, specialtycode, provtype, fedid, fullname, exthours, profdesig, status, ethnicid, servlocation, taxexempt, medicarepar, upin, credentialstatus, gpciid, provwatch, createid, createdate, lastupdate, updateid, xtrnid, pin, handicapaccess, sex, dob, overrideroleid, lienmailtomember, rentalnetwork, autocreated, autoupdated, npi, externalediting, FedIdType, PoaExempt, FiscalYearEndMonth, FiscalYearEndDay, Ssn, ExternalId, IhsProvider, state, year, source_state as part_state, job_date, part_state as ods_part_state, part_year from """+qnxt_database+""".provider""")
*/ //removing ods_part_state dlam 3806
 var provider = spark.sql("""select trim(provid) as provid, trim(entityid) as entityid, trim(specialtycode) as specialtycode, provtype, fedid, fullname, exthours, profdesig, status, ethnicid, servlocation, taxexempt, medicarepar, upin, credentialstatus, gpciid, provwatch, createid, createdate, lastupdate, updateid, xtrnid, pin, handicapaccess, sex, dob, overrideroleid, lienmailtomember, rentalnetwork, autocreated, autoupdated, npi, externalediting, FedIdType, PoaExempt, FiscalYearEndMonth, FiscalYearEndDay, Ssn, ExternalId, IhsProvider, state, year, trim(source_state) as part_state, job_date, part_year from """+qnxt_database+""".provider""")
 
//.filter("part_state ='" + StateCode + "'")
/*provider = provider.withColumn("rnk", row_number().over(Window.partitionBy("part_state", "provid").orderBy(desc("lastupdate")))).filter("rnk=1").drop("rnk")-removed - infinite dlam 1529
var provider_del = spark.table("qnxt_db.provider_del") //.filter("part_state ='" + StateCode + "'")
var provider_df = provider.as("a").join(provider_del.as("b"), col("a.part_state") === col("b.part_state") && col("a.provid") === col("b.provid") && (col("a.createdate") === col("b.createdate") || col("a.lastupdate") === col("b.lastupdate")), "left").select(col("a.provid"), col("a.part_state"),col("b.provid").as("b_provid")).filter("b_provid is Null").distinct-removed - infinite dlam 1529*/
 
 
//val prov_spec_join = spec_join.as("p").join(provider_df.as("sp1"), col("p.provid") === col("sp1.provid") && col("p.part_state") === col("sp1.part_state") ).selectExpr("p.*") -replaced provider_df as provider - infinite dlam 1529
val prov_spec_join = spec_join.as("p").join(provider.as("sp1"), trim(col("p.provid")) === trim(col("sp1.provid")) && col("p.part_state") === col("sp1.part_state") ).selectExpr("p.*")
//prov_spec_join.createTempView("columns")-replaced temp view as ReplaceTempView - infinite dlam 1529
prov_spec_join.createOrReplaceTempView("columns")
 
//var final_tmp_columns = spark.sql("select distinct  provid,specialtycode,effdate,termdate,description,part_state source_state, spectype as provider_specialtytype from columns")
//var final_tmp_columns = spark.sql("select distinct  trim(provid) as provider_id,trim(specialtycode) as provider_specialtycode,effdate as provider_specialtyeffdate,termdate as provider_specialtytermdate,trim(description) as provider_specialtydesc,part_state as source_state, trim(spectype) as provider_specialtytype from columns")-added source_system column for partition - infinite dlam sprint9
/*
var final_tmp_columns = spark.sql("select distinct  trim(provid) as provider_id,trim(specialtycode) as provider_specialtycode,effdate as provider_specialtyeffdate,termdate as provider_specialtytermdate,trim(description) as provider_specialtydesc,part_state as source_state, trim(spectype) as provider_specialtytype,'qnxt' as source_system from columns")
*/ //removing distinct for DQ bug-- DLAM
//Using distinct for bug dlam 3803
//Code added as part of RA project
var final_tmp_columns = spark.sql("select distinct trim(provid) as provider_id,trim(specialtycode) as provider_specialtycode,effdate as provider_specialtyeffdate,termdate as provider_specialtytermdate,trim(description) as provider_specialtydesc,part_state as source_state, trim(spectype) as provider_specialtytype, specialtystatus as provider_specialtystatus, lastupdate as provider_specialty_lastupdate, createdate as provider_specialty_createdate,'qnxt' as source_system from columns")
 
//reverting back to remove ods_part_state dlam 3806
//--Adding ods_part_state for sprint 17 DLAM
/*var final_tmp_columns = spark.sql("select distinct  trim(provid) as provider_id,trim(specialtycode) as provider_specialtycode,effdate as provider_specialtyeffdate,termdate as provider_specialtytermdate,trim(description) as provider_specialtydesc,part_state as source_state, trim(spectype) as provider_specialtytype,'qnxt' as source_system, ods_part_state from columns") */
 
//final_tmp_columns.createTempView("final_spec")-replaced temp view as ReplaceTempView - infinite dlam 1529
final_tmp_columns.createOrReplaceTempView("final_spec")
 

// COMMAND ----------

//spark.sql("""CREATE EXTERNAL TABLE if not exists enterprise_db.provider_specialty  (provider_id string , provider_specialtycode string , provider_specialtyeffdate timestamp , provider_specialtytermdate timestamp , provider_specialtydesc string )   PARTITIONED BY (source_state string,provider_specialtytype string )  STORED AS PARQUET LOCATION '/edl/transform/enterprise/data/provider_specialty'""")
 
//spark.sql("""CREATE TABLE if not exists enterprise_db.provider_specialty  (provider_id string , provider_specialtycode string , provider_specialtyeffdate timestamp , provider_specialtytermdate timestamp , provider_specialtydesc string )  Using delta PARTITIONED BY (source_state string,provider_specialtytype string )  LOCATION 'dbfs:/mnt/eimuatadlsg2/datalake/TransformZone/enterprise_db/data/provider_specialty'""")//-replaced path and table- infinite dlam 1529
 
//spark.sql("insert overwrite table enterprise_db.provider_specialty partition(source_state,provider_specialtytype) select distinct * from final_spec")-replaced logic to insert data- infinite DLAM-1529
//added source_system as partition column - infinite DLAM-2249
//final_tmp_columns.write.format("delta").option("mergeSchema","true").mode("overwrite").partitionBy("source_state","source_system","provider_specialtytype").saveAsTable("enterprise_db.provider_specialty") Changing db to provider_db for DLAM sprint 16
final_tmp_columns.write.format("delta").option("mergeSchema","true").mode("overwrite").partitionBy("source_state","source_system","provider_specialtytype").saveAsTable("provider_db.provider_specialty")
   /* }
    catch {
      case e: Exception => { e.printStackTrace() }
    }
  }
}*/
 

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from provider_db.provider_specialty