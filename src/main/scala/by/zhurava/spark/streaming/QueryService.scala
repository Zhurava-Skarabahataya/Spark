package by.zhurava.spark
package by.zhurava.spark.streaming

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions.{col, datediff, expr, greatest, lit, struct, sum, when}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * Service for performing queries with SparkSQL.
 */
object QueryService {

  val mostPopularStayType = "most_popular_stay_type"
  val duration = "duration"
  val celsius = "celsius"
  val checkIn = "srch_ci"
  val checkOut = "srch_co"
  val id = "id"
  val hotelId = "hotel_id"
  val bookingChildrenCount = "srch_children_cnt"
  val childrenCount = "children_count"
  val withChildren = "with_children"
  val name = "name"
  val joinType = "inner"

  val countStayTypeQuery = "SELECT hotel_id, name, " +
    "sum (srch_children_cnt) as children_count, " +
    "count (case when(duration>=30 or cast(duration as int) <= 0 or duration is null) " +
    "then 1 else cast (NULL as int) end) as erroneous_data_cnt, " +
    "count (case when(duration= 1) then 1 else cast (NULL as int) end) as short_stay_cnt, " +
    "count (case when(duration BETWEEN 2 AND 7) then 1 else cast (NULL as int) end) as standart_stay_cnt, " +
    "count (case when(duration BETWEEN 7 AND 14) then 1 else cast (NULL as int) end) as standart_extended_stay_cnt, " +
    "count (case when(duration BETWEEN 14 AND 30) then 1 else cast (NULL as int) end) as long_stay_cnt " +
    "FROM bookings group by hotel_id, name;"

  val unionQuery = "SELECT ds_left.hotel_id as hotel_id, ds_left.name as hotel_name, " +
    "ds_left.children_count + ds_right.children_count as children_count, " +
    "ds_left.erroneous_data_cnt + ds_right.erroneous_data_cnt as erroneous_data_cnt, " +
    "ds_left.short_stay_cnt + ds_right.short_stay_cnt as short_stay_cnt, " +
    "ds_left.standart_stay_cnt + ds_right.standart_stay_cnt as standart_stay_cnt, " +
    "ds_left.standart_extended_stay_cnt + ds_right.standart_extended_stay_cnt as standart_extended_stay_cnt, " +
    "ds_left.long_stay_cnt + ds_right.long_stay_cnt as long_stay_cnt " +
    "FROM ds_left JOIN ds_right ON ds_left.hotel_id = ds_right.hotel_id;"

  /**
   * Method counts different types of stay.
   * @param expedia {@link DataFrame} with expedia data
   * @param spark {@link SparkSession} that configurates the stream
   * @return {@link DataFrame} with counted types of stay for each hotel
   */
  def countTypeOfStay(expedia: DataFrame, spark: SparkSession): DataFrame = {
    expedia.createOrReplaceTempView("bookings")
    spark.sql(countStayTypeQuery)
  }

  /**
   * Method joins expedia data with temperature
   * @param expedia {@link Dataset} with expedia data
   * @param hotelsAndWeather {@link Dataset} with hotels and weather data
   * @return {@link DataFrame} with temperature added to expedia
   */
  def addWeatherToExpedia(expedia: Dataset[Row], hotelsAndWeather: Dataset[Row]): DataFrame = {
    joinDatasetsExpediaAndWeather(expedia, hotelsAndWeather).select(id, bookingChildrenCount, checkIn, checkOut, hotelId, name, celsius)
  }

  /**
   * Method joins expedia and weather data by hotel id
   * @param expedia {@link Dataset} with expedia data
   * @param hotelsAndWeather
   * @return {@link DataFrame} of joined weather and weather
   */
  def joinDatasetsExpediaAndWeather(expedia: Dataset[Row], hotelsAndWeather: Dataset[Row]): DataFrame = {
    expedia.join(hotelsAndWeather
      .withColumnRenamed(id, "id_hotel"), expr(""" hotel_id = id_hotel and srch_ci = `date`"""), joinType)
  }

  /**
   * Method filters records only with temperature more then 0 Celsius
   * @param records {@link DataFrame} with expedia data
   * @return Records only with temperature more then 0 Celsius
   */
  def filterUnderZeroTmpRecords(records: DataFrame): Dataset[Row] = {
    records.filter(records(celsius).gt(0))
  }

  /**
   * Method counts duration of stay for each booking record
   * @param expedia {@link Dataset} with expedia data
   * @return {@link DataFrame} with duration of stay for each booking record
   */
  def countDurationOfStay(expedia: Dataset[Row]): DataFrame = {
    expedia.withColumn(duration, datediff(col(checkOut), col(checkIn)))
  }

  /**
   * Method defines the most popular type of stay.
   * @param records {@link DataFrame} of hotels with counted type of stay
   * @return {@link Dataset} with the most popular type of stay counted
   */
  def findMostPopularTypeOfStay(records: DataFrame): Dataset[Row] = {
    val stayColumns = records.columns.takeRight(5).map(
      c => struct(col(c).as("value"), lit(c).as("key"))
    )
    records
      .withColumn(mostPopularStayType, greatest(stayColumns: _*).getItem("key"))
  }

  /**
   * Method process datasets: jons two datasets, counts the most popular type of stay and defines children presence.
   * @param dataset2016 {@link Dataset} with expedia data for 2016
   * @param dataset2017 {@link Dataset} with expedia data for 2017
   * @param spark {@link SparkSession} that configurates the stream
   * @return Final {@Dataset}
   */
  def createFinalDataset(dataset2016: Broadcast[Dataset[Row]], dataset2017: Dataset[Row], spark: SparkSession): Dataset[Row] = {
    val unionedDatasets = unionStreamAndBatch(dataset2016.value, dataset2017, spark)

    val mostPopularTypeOfsStayCounted = findMostPopularTypeOfStay(unionedDatasets)

    val childrenPresenceDS = defineChildrenPresence(mostPopularTypeOfsStayCounted)

    Utils.addTimestamp(childrenPresenceDS)

  }

  /**
   * Method adds column with information about children presence in bookings
   * @param dataframe {@link DataFrame} with hotels and children count
   * @return {@link Dataset} with added column with information about children presence in bookings
   */
  def defineChildrenPresence(dataframe: DataFrame): Dataset[Row] = {
    dataframe.withColumn(withChildren, when(col(childrenCount) > 0, true).when(col(childrenCount) <= 0, false))
      .drop(col(childrenCount))
  }

  /**
   * Method joins two datasets for different years with counting types of stay.
   * @param datasetLeft {@link Dataset} with expedia data
   * @param datasetRigth {@link Dataset} with expedia data
   * @param spark {@link SparkSession} that configurates the stream
   * @return {@link DataFrame} with stream of expedia data for 2016 and 2017 with total count of different types of stay counted
   */
  def unionStreamAndBatch(datasetLeft: Dataset[Row], datasetRigth: Dataset[Row], spark: SparkSession): DataFrame = {
    datasetLeft.createOrReplaceTempView("ds_left")
    datasetRigth.createOrReplaceTempView("ds_right")

    spark.sql(unionQuery)

  }

}
