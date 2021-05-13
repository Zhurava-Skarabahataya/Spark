package by.zhurava.spark
package by.zhurava.spark.streaming

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.{OutputMode}

/**
 * Object which makes transformations wih expedia data from "Spark Batching Homework"
 */
object Main {

  val kafkaTopic = "hotels_and_weather"
  val topicOption = "subscribe"
  val expediaSource2016 = "hdfs://localhost:9000/homework_spark_win/year=2016/*"
  val expediaSource2017 = "hdfs://localhost:9000/homework_spark_win/year=2017/*"
  val expediaFormat = "avro"
  val checkpointLocation = "hdfs://localhost:9000/check/1/"
  val resultPath = "hdfs://localhost:9000/streaming_result_parquet"

  /**
   * Starting point of the application.
   * @param args Arguments passed to the app.
   */
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("SparkStreaming")
      .config("spark.sql.shuffle.partitions", 10)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    //Read Expedia data for 2016 year from HDFS on WSL2
    val expedia2016DF = spark.read
      .format("avro")
      .load(expediaSource2016)

    //Reading hotels and weather data from kafka topic
    val weatherAndHotelsFromKafkaDF = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9094")
      .option(topicOption, kafkaTopic)
      .load()

    //Creating schema for kafka data
    val schemaForKafka = Utils.createSchemaForKafkaHotels()

    //Enrich data for 2016 with weather: add average temperature at checkin (join with hotels+weaher data from Kafka topic).
    val weatherWithHotelsDF = weatherAndHotelsFromKafkaDF
      .selectExpr("CAST(value as STRING)")
      .withColumn("data", from_json(col("value"), schemaForKafka))
      .as("weather")
      .select("data.*")
      .alias("hotels_and_weather")

    //Enrich expedia data for 2016 with weather: add average temperature at checkin (join with hotels+weather data from Kafka topic).
    val expediaWithWeatherAtCheckin2016DF = QueryService.addWeatherToExpedia(expedia2016DF, weatherWithHotelsDF)

    //Filter incoming data by having average temperature more than 0 Celsius degrees
    val expediaFilteredTemperature2016DF = QueryService.filterUnderZeroTmpRecords(expediaWithWeatherAtCheckin2016DF)

    //Calculate customer's duration of stay as days between requested check-in and check-out date.
    val expediaStayDaysCalculated2016DF = QueryService.countDurationOfStay(expediaFilteredTemperature2016DF)

    //Calculate type of stay.
    val expediaStayTypeCounted2016DF = QueryService.countTypeOfStay(expediaStayDaysCalculated2016DF, spark)

    //Add most_popular_stay_type for a hotel (with max count)
    val expediaMostPopularStayTypeCounted2016DF = QueryService.findMostPopularTypeOfStay(expediaStayTypeCounted2016DF)

    //Adding timestamp.
    val finalDataset2016 = Utils.addTimestamp(expediaMostPopularStayTypeCounted2016DF)

    //Broadcast dataset.
    val broadcastedDataset2016 = spark.sparkContext.broadcast(finalDataset2016)

    //Reading schema for  expedia data.
    val schemaFromHdfs = spark.read
      .format(expediaFormat)
      .load(expediaSource2017)
      .schema

    //Reading expedia data.
    val expedia2017DF = spark.readStream.format(expediaFormat)
      .option("path", expediaSource2017)
      .option("stream.read.batch.size", 5)
      .option("maxFilesPerTrigger", 1)
      .option("spark.streaming.backpressure.enabled", true)
      .schema(schemaFromHdfs)
      .load()

    //Repeat previous logic on the stream
    //enrich with weather add average temperature at checkin (join with hotels+weaher data from Kafka topic).
    val expediaWithWeatherAtCheckin2017DF = QueryService.addWeatherToExpedia(expedia2017DF, weatherWithHotelsDF)

    //Filter incoming data by having average temperature more than 0 Celsius degrees.
    val expediaFilteredTemperature2017DF = QueryService.filterUnderZeroTmpRecords(expediaWithWeatherAtCheckin2017DF)

    //Calculate customer's duration of stay as days between requested check-in and check-out date.
    val expediaStayDaysCalculated2017DF = QueryService.countDurationOfStay(expediaFilteredTemperature2017DF)

    //Create customer preferences of stay time based on next logic.
    //Map each hotel with multi-dimensional state consisting of record counts for each type of stay.
    val expediaStayTypeCounted2017DF = QueryService.countTypeOfStay(expediaStayDaysCalculated2017DF, spark)

    //Find most popular type of stay
    val expediaMostPopularStayTypeCounted2017DF = QueryService.findMostPopularTypeOfStay(expediaStayTypeCounted2017DF)

    //Adding timestamp for stream
    val finalDataset2017 = Utils.addTimestamp(expediaMostPopularStayTypeCounted2017DF)

    //Union stream and batch and apply additional variance with filter on children presence in booking
    // (with_children: false - children <= 0; true - children > 0).
    val finalStream = QueryService.createFinalDataset(broadcastedDataset2016, finalDataset2017, spark)

    //Writing final stream to HDFS
    finalStream
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write.mode(SaveMode.Append)
          .format("parquet")
          .format("parquet")
          .option("path", resultPath)
          .save()
      }
      //.format("console")
      .option("checkpointLocation", checkpointLocation)
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()

  }

}
