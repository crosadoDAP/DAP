package com.hbo.dma.hulu.huludata

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * This object will read bson file and create parquet file at target location.
  * File has been cleared for public use
  * @version 1.2
  * @author Charlie Rosado 08/06/2018
  */

/* Hulu data processing -
* creates a new column called 'service' that extracts the source of the feed from filename.
* The only method to determine the source of the data (HBO vs Cinemax) is to extract the source from filename.
* The process herein scans the filename, identifies whether HBO or Cinemax and then places the value in a new column.
* Same rule is going to apply for partitioning.
* This file impacts the HuluData set which represents Hulu_User_Daily
*/

object HuluDaily {

  //spark session
  //keep it transient for avoiding broad cast error. Once there are two spark this issue arise.
  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .config("spark.master", "yarn") //when cluster/client mode
    // .config("spark.master", "local[*]") //for local only
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    checkArguments(args)
    //Argument to read source data when runinng spark-submit
    val input_path = args(0)
    //Argument to read target folder when runinng spark-submit
    val output_path = args(1)
    //Argument  to read number partitions
    val numPart = args(2).toInt

    val rawData = spark.read.option("header", "true").option("delimiter", "|").csv(input_path)
    val dataFrame = rawData
      .withColumn("last_2_parts_of_filename_tmp_col", {
      val file_name_parts = split(input_file_name(), "/")
      val file_name_only = file_name_parts(size(file_name_parts) - 1)

      val lowerCaseHboOrMax = split(file_name_only, "_")(0)

      val service = when(lowerCaseHboOrMax === "hbo", lit("HBO")).otherwise(initcap(lowerCaseHboOrMax))

      val date_include_dot_text = split(file_name_only, "-")(1)
      val date_only = split(date_include_dot_text, "\\.")(0)
      val last_2_parts = concat(service, lit("~"), date_only)
      last_2_parts
    })
      .withColumn("service", split(col("last_2_parts_of_filename_tmp_col"), "~")(0))
      .transform(HuluSpoc.withPartitionColumn)
      //remove temporary column
      .drop("last_2_parts_of_filename_tmp_col")

    dataFrame.createTempView("hulu_dayily")
    val finalOutputDF = spark.sqlContext.sql(
      """
        |select cast(RealDateWithHour as TIMESTAMP) real_date_with_hour, DeviceCategoryName device_category_name, HuluHashedID hulu_hashed_id,
        |   cast(age as INT) age, gender, ZipCodeSubsciptionAddress zipcode_subscription_address, ZipCodeCurrentLocation zipcode_current_location,
        |   cast(TimeWatched_min as DECIMAL(20,12))  timewatched_min, bundle, ContentPartner content_partner, BasePlan base_plan, SeriesTitle series_title,
        |   season, EpisodeTitle episode_title, EpisodeNumber episode_number, format, service, yr, mo, dt from hulu_dayily
      """.stripMargin)
    finalOutputDF.printSchema()
    finalOutputDF.repartition(numPart).write.
      mode(SaveMode.Overwrite).
      parquet(output_path)
  }

  /**
    * Check main argument. System will exit if given input is not provided.
    *
    * @param args
    * Array[String]
    */
  def checkArguments(args: Array[String]) = {
    if (args == null || args.length < 3) {
      println("Input, output s3 path and number of partition values are required. Usage: s3://bucket/input/ s3://bucket/output/ 1")
      System.exit(1)
    }
  }

}

