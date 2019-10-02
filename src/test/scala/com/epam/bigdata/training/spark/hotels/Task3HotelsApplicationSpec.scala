//package com.epam.bigdata.training.spark.hotels
//
//import java.util.UUID
//
//import com.epam.bigdata.training.spark.hotels.booking.{Booking, CompositeHotelId}
//import com.github.mrpowers.spark.fast.tests.DatasetComparer
//import org.apache.spark.sql.SparkSession
//import org.scalatest.{FeatureSpec, GivenWhenThen}
//
//import scala.io.Source
//import scala.reflect.io.File
//
//class Task3HotelsApplicationSpec extends FeatureSpec with DatasetComparer with GivenWhenThen {
//
//  lazy val spark: SparkSession = {
//    SparkSession.builder().master("local").appName("Test spark session").getOrCreate()
//  }
//
//  feature("Dataset loading") {
//
//    scenario("Loading from the specified input") {
//      Given("src/test/resources/train-cutted.csv as an input source")
//
//      When("trying to load it")
//      val result = Task3HotelsApplication.loadCsvDataset(spark, "src/test/resources/train-cutted.csv")
//
//      Then("expect dataset to be loaded correctly and typed to Booking")
//      assert(result.count == 10)
//
//      And("first row to match the expected one")
//      assert(result.head(1)(0) == Booking(0, 2, 0, 66, 2, 50, 628))
//    }
//
//  }
//
//  feature("Top3 hotels calculation check") {
//
//    scenario("Results in empty dataset if all booked") {
//      import spark.implicits._
//
//      Given("Dataset with all eventually booked interests")
//      val dataset = spark.createDataset(Seq(
//        Booking(1, 1, 2, 66, 2, 50, 628),
//        Booking(1, 1, 0, 66, 2, 50, 628),
//        Booking(1, 1, 1, 66, 2, 50, 675)
//      ))
//
//      When("finding top 3 most popular hotels people with children eventually not booked")
//      val result = Task1HotelsApplication.findTop3MostPopularHotelsBetweenCouples(spark, dataset)
//
//      Then("expect to get an empty dataset")
//      assert(result.count == 0)
//    }
//
//    scenario("Results in empty dataset if all without children") {
//      import spark.implicits._
//
//      Given("Dataset with all bookings without children")
//      val dataset = spark.createDataset(Seq(
//        Booking(0, 1, 2, 66, 2, 50, 628),
//        Booking(0, 1, 3, 66, 2, 50, 628),
//        Booking(0, 1, 1, 66, 2, 50, 675)
//      ))
//
//      When("finding top 3 most popular hotels people with children eventually not booked")
//      val result = Task1HotelsApplication.findTop3MostPopularHotelsBetweenCouples(spark, dataset)
//
//      Then("expect to get an empty dataset")
//      assert(result.count == 0)
//    }
//
//    scenario("Calculates counts correctly by grouping by composite hotel id and filtering out booked or without children") {
//      import spark.implicits._
//
//      Given("Dataset with bookings made by singles only")
//      val dataset = spark.createDataset(Seq(
//        Booking(0, 2, 2, 66, 2, 50, 628),
//        Booking(0, 2, 0, 66, 2, 50, 628), // no children
//        Booking(0, 2, 1, 66, 2, 50, 675),
//        Booking(1, 1, 1, 66, 2, 50, 675), // booked
//        Booking(0, 2, 0, 66, 2, 50, 628), // no children
//        Booking(0, 2, 1, 66, 2, 50, 675),
//        Booking(0, 2, 3, 66, 2, 50, 675),
//        Booking(0, 2, 1, 66, 2, 50, 655),
//        Booking(0, 2, 3, 66, 2, 50, 655),
//        Booking(0, 2, 1, 66, 2, 50, 678)
//      ))
//
//      When("finding top 3 most popular hotels by peiople with chidlren and not booked")
//      val result = Task3HotelsApplication.findTop3InterestedByPeopleWithChildrenButNotBooked(spark, dataset)
//
//      Then("expect to get a dataset of 3 top entries")
//      assert(result.count == 3)
//      assert(result.take(3)(0) == (CompositeHotelId(2, 50, 675), 3L))
//      assert(result.take(3)(1) == (CompositeHotelId(2, 50, 655), 2L))
//      assert(result.take(3)(2) == (CompositeHotelId(2, 50, 628), 1L))
//    }
//  }
//
//  feature("Outputting to multiple outputs") {
//
//    scenario("Outputting to console and specified csv file") {
//      import spark.implicits._
//
//      Given("Resulting dataset of hotels and their popularity and target csv path")
//      val dataset = spark.createDataset(Seq(
//        (CompositeHotelId(2, 50, 628), 2L),
//        (CompositeHotelId(2, 50, 675), 1L)
//      ))
//      val path = "target/task-3-test-output/" + UUID.randomUUID().toString + ".csv"
//
//      When("calling output function")
//      Task3HotelsApplication.outputResult(spark, dataset, path)
//
//      Then("expect the file to be written with proper content")
//      val file = File(path).toDirectory.files.filter(_.extension == "csv").toSeq.head
//      val lines = Source.fromFile(file.path).getLines.toSeq
//      assert(lines == Seq("2,50,628,2", "2,50,675,1"))
//    }
//  }
//
//}
