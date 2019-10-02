package com.epam.bigdata.training.spark.hotels

import com.epam.bigdata.training.spark.hotels.booking.Booking
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}

/**
  * The task is the following:
  *
  * <p />
  * Find the most popular country where hotels are booked and searched from the same country.
  */
object Task2HotelsApplication {

  def main(args: Array[String]): Unit = {

    if (args.length != 2)
      throw new IllegalArgumentException("Expected input source and output to be specified")

    val in: String = args(0)
    val out: String = args(1)

    // Create a SparkContext to initialize Spark
    val spark = SparkSession.builder
      .appName("Spark Task 2 Application: the most popular country where hotels are booked and searched from the same country")
      .getOrCreate()

    // load dataset
    val bookingsData = loadCsvDataset(spark, in)

    // now, find most popular
    val topHotels = findMostPopularHotelBookedAndSearchedFromSameCountry(spark, bookingsData)

    // now, output it to the specified location
    outputResult(spark, topHotels, out)

    spark.stop()
  }

  /**
    * Loads csv dataset from the specified path.
    * @param spark  Spark Session.
    * @param path   Path to load the dataset from.
    * @return Booking dataset.
    */
  def loadCsvDataset(spark: SparkSession, path: String): Dataset[Booking] = {
    import spark.implicits._

    val schema = Encoders.product[Booking].schema
    val columns = schema.fieldNames.map(new Column(_))

    spark.read
      .option("header", "true")       //read the headers
      .option("inferSchema", "true")  // restore the schema from headers
      .csv(path)
      .select(columns:_*)             // select only the specified columns
      .as[Booking]                    // to typed dataset
  }

  /**
    * Filters out the incoming dataset by leaving only booked hotels,
    * then filters out by leaving only with matching country,
    * then groups by hotel country,
    * counts each group size,
    * then orders in descending order,
    * and finally limits the dataset to top row
    * @param bookings Initial bookings dataset.
    * @return Dataset consisting of top hotel country and their popularity.
    */
  def findMostPopularHotelBookedAndSearchedFromSameCountry(spark: SparkSession, bookings: Dataset[Booking]): Dataset[(Int, Long)] = {
    import org.apache.spark.sql.functions._
    import spark.implicits._

    bookings
      .filter(_.booked)                     // leave booked only
      .filter(booking => booking.user_location_country == booking.hotel_country) // leave bookings from same country
      .groupByKey(_.hotel_country)         // group by country
      .count                          // count each group size
      .orderBy(desc("count(1)"))  // order descending
      .limit(1)                        // get most popular
  }

  /**
    * Outputs the resulting dataset in the specified location in csv format.
    * The dataset is repartitioned first to 1 partition in order to write into a single file.
    * @param dataset  Resulting dataset to output.
    * @param out      Out path.
    */
  def outputResult(spark: SparkSession, dataset: Dataset[(Int, Long)], out: String): Unit = {

    val ds = dataset
      .cache()                  // cache to write the result to multiple output sources without recomputations

    ds.count() // to cache the data physically

    // write to out path first
    ds
      .coalesce(1) // to write to a single file
      .write
      .format("csv")
      .save(out)

    ds
      .write
      .format("console")
      .save()
  }
}
