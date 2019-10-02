package com.epam.bigdata.training.spark.hotels

import com.epam.bigdata.training.spark.hotels.booking.{Booking, CompositeHotelId, RawBooking}
import org.apache.spark.sql.{Column, Dataset, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel

/**
  * The task is the following:
  *
  * <br />
  * Find top 3 most popular hotels between couples.
  * (treat hotel as composite key of continent, country and market).
  * Implement using scala or python. Create a separate application.
  * Copy the application to the archive.
  * Make screenshots of results: before and after execution.
  */
object Task1HotelsApplication {

  def main(args: Array[String]): Unit = {

    if (args.length != 2)
      throw new IllegalArgumentException("Expected input source and output to be specified")

    val in: String = args(0)
    val out: String = args(1)

    // Create a SparkContext to initialize Spark
    val spark = SparkSession.builder
      .appName("Spark Task 1 Application: top 3 most popular hotels between couples")
      .getOrCreate()

    // load dataset
    val bookingsData = loadCsvDataset(spark, in)

    // now, find top 3 most popular hotels
    val topHotels = findTop3MostPopularHotelsBetweenCouples(spark, bookingsData)

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

    val schema = Encoders.product[RawBooking].schema
    val columns = schema.fieldNames.map(new Column(_))

    spark.read
//        .schema(schema)
      .option("header", "true")       //read the headers
      //.option("inferSchema", "true")  // restore the schema from headers
      .csv(path)
      .select(columns:_*)             // select only the specified columns
      .as[RawBooking]                    // to typed dataset
      .map(_.asBooking)
  }

  /**
    * Filters out the incoming dataset by leaving couples only,
    * then groups by hotel composite id,
    * counts each group size,
    * then orders in descending order,
    * and finally limits the dataset to top 3 rows.
    * @param bookings Initial bookings dataset.
    * @return Dataset consisting of top hotel identifiers and their popularity.
    */
  def findTop3MostPopularHotelsBetweenCouples(spark: SparkSession, bookings: Dataset[Booking]): Dataset[(CompositeHotelId, Long)] = {
    import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount}
    import org.apache.spark.sql.functions._
    import spark.implicits._

    bookings
      .filter(_.srch_adults_cnt == 2) // leave couples only
      .groupByKey(_.hotel_id)         // group by hotel id
      .count // count each group size
      .orderBy(desc("count(1)"))  // order descending
      .limit(3)                       // get top 3 rows
  }

  /**
    * Outputs the resulting dataset in the specified location in csv format.
    * The dataset is repartitioned first to 1 partition in order to write into a single file.
    * @param dataset  Resulting dataset to output.
    * @param out      Out path.
    */
  def outputResult(spark: SparkSession, dataset: Dataset[(CompositeHotelId, Long)], out: String): Unit = {
    import spark.implicits._

    val ds = dataset
      .cache()                  // cache to write the result to multiple output sources without recomputations

    ds.count // to force cache actually work

    // write to out path first
    ds
      .map(pair => (pair._1.hotel_continent, pair._1.hotel_country, pair._1.hotel_market, pair._2))
      //.coalesce(1) // to write to a single file
      .write
      .format("csv")
      .save(out)

    ds
      .write
      .format("console")
      .save()

    ds.unpersist
  }

}
