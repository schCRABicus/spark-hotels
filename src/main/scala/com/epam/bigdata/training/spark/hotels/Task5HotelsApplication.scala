package com.epam.bigdata.training.spark.hotels

import com.epam.bigdata.training.spark.hotels.booking.{Booking, CompositeHotelId}
import org.apache.spark.sql._

/**
  * The task is the following:
  *
  * <p />
  * Find top 3 hotels where people with children are interested but not booked in the end.
  */
object Task5HotelsApplication {

  def main(args: Array[String]): Unit = {

    if (args.length != 2)
      throw new IllegalArgumentException("Expected input source and output to be specified")

    val in: String = args(0)
    val out: String = args(1)

    // Create a SparkContext to initialize Spark
    val spark = SparkSession.builder
      .appName("Spark Task 3 Application: Find top 3 hotels where people with children are interested but not booked in the end")
      .config("spark.sql.parquet.binaryAsString", "true")
      .getOrCreate()

    // load dataset
    val bookingsData = loadDataset(spark, in)

    // now, find top 3 interested by people with children and not booked in the end
    //val topHotels = findTop3InterestedByPeopleWithChildrenButNotBooked(spark, bookingsData)

    // now, output it to the specified location
    outputResult(spark, bookingsData, out)

    spark.stop()
  }

  /**
    * Loads dataset from the specified path.
    * @param spark  Spark Session.
    * @param path   Path to load the dataset from.
    * @return Booking dataset.
    */
  def loadDataset(spark: SparkSession, path: String): DataFrame = {
    import org.apache.spark.sql.functions.col
    import spark.implicits._

    val schema = Encoders.product[Booking].schema
    val columns = schema.fieldNames.map(new Column(_))

    val df = spark.read
        //.schema(schema)
      //.option("mergeSchema", "true")
      //.option("header", "true")       //read the headers
      //.option("inferSchema", "true")  // restore the schema from headers
      .parquet(path)

    df.show()
    df.printSchema()

    df
  }

  /**
    * Filters out the incoming dataset by leaving booked only,
    * then filters out adults without children,
    * then groups by hotel composite id,
    * counts each group size,
    * then orders in descending order,
    * and finally limits the dataset to top 3 rows.
    * @param bookings Initial bookings dataset.
    * @return Dataset consisting of top hotel identifiers and their popularity.
    */
  def findTop3InterestedByPeopleWithChildrenButNotBooked(spark: SparkSession, bookings: Dataset[Booking]): Dataset[(CompositeHotelId, Long)] = {
    import org.apache.spark.sql.expressions.scalalang.typed.{count => typedCount}
    import org.apache.spark.sql.functions._
    import spark.implicits._

    bookings
      .filter(!_.booked)                   // leave not booked only
      .filter(_.srch_children_cnt > 0)     // people with children only
      .groupByKey(_.hotel_id)              // group by composite hotel id
      .agg(typedCount[Booking](_.hotel_id).name("popularity")) // count each group size
      .orderBy(desc("popularity"))  // order descending
      .limit(3)                            // get top 3 rows
  }

  /**
    * Outputs the resulting dataset in the specified location in csv format.
    * The dataset is repartitioned first to 1 partition in order to write into a single file.
    * @param dataset  Resulting dataset to output.
    * @param out      Out path.
    */
  def outputResult(spark: SparkSession, df: DataFrame, out: String): Unit = {
    import spark.implicits._

//    val ds = dataset
//      .cache()                  // cache to write the result to multiple output sources without recomputations
//
//    ds.count

    // write to out path first
    df
//      .map(pair => (pair._1.hotel_continent, pair._1.hotel_country, pair._1.hotel_market, pair._2))
      .coalesce(1) // to write to a single file
      .write
      .format("csv")
      .save(out)

//    ds
//      .write
//      .format("console")
//      .save()
  }
}
