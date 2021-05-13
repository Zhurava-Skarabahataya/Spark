package by.zhurava.spark
package by.zhurava.spark.streaming

import org.apache.spark.sql.functions.{current_timestamp, format_string}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructType}

/**
 * Object that performs additional logic.
 */
object Utils {

  //Creates schema for data in Kafka topic with hotels and weather.
  /**
   * Method for creating schema for data in kafka topic.
   * @return Schema for hotels and weather data in kafka topic.
   */
  def createSchemaForKafkaHotels(): StructType = {
    new StructType()
      .add("id", StringType)
      .add("name", StringType)
      .add("country", StringType)
      .add("city", StringType)
      .add("address", StringType)
      .add("latitude", DoubleType)
      .add("longitude", DoubleType)
      .add("geoHash", StringType)
      .add("date", StringType)
      .add("celsius", DoubleType)
      .add("fahrenheit", DoubleType)
      .add("hasWeather", BooleanType)
  }

  /**
   * Method that adds timestamp for rows in dataset.
   * @param dataset Dataset that needs timestamp.
   * @return Dataset with timestamp added.
   */
  def addTimestamp(dataset: Dataset[Row]): Dataset[Row] = {
    dataset.withColumn("batch_timestamp", current_timestamp())
    //dataset.withColumn("batch_timestamp", format_string(java.time.LocalDateTime.now().toString))
  }

}
