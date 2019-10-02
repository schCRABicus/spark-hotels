package com.epam.bigdata.training.spark.hotels

import java.util.UUID

import com.epam.bigdata.training.spark.hotels.booking.{Booking, CompositeHotelId}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.io.Source
import scala.reflect.io.File

class Task2HotelsApplicationSpec extends FeatureSpec with GivenWhenThen {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local").appName("Test spark session").getOrCreate()
  }

  feature("Dataset loading") {

    scenario("Loading from the specified input") {
      Given("src/test/resources/train-cutted.csv as an input source")

      When("trying to load it")
      val result = Task2HotelsApplication.loadCsvDataset(spark, "src/test/resources/train-cutted.csv")

      Then("expect dataset to be loaded correctly and typed to Booking")
      assert(result.count == 10)

      And("first row to match the expected one")
      assert(result.head(1)(0) == Booking(0, 2, 0, 66, 2, 50, 628))
    }

  }

  feature("Most popular hotels with same booking country calculation check") {

    scenario("Results in empty dataset if no booked") {
      import spark.implicits._

      Given("Dataset with bookings not made eventually")
      val dataset = spark.createDataset(Seq(
        Booking(0, 1, 2, 66, 2, 50, 628),
        Booking(0, 1, 0, 66, 2, 50, 628),
        Booking(0, 1, 1, 66, 2, 50, 675)
      ))

      When("finding most popular booked hotels with same country")
      val result = Task2HotelsApplication.findMostPopularHotelBookedAndSearchedFromSameCountry(spark, dataset)

      Then("expect to get an empty dataset")
      assert(result.count == 0)
    }

    scenario("Results in empty dataset if no bookings with same country") {
      import spark.implicits._

      Given("Dataset without bookings within same country")
      val dataset = spark.createDataset(Seq(
        Booking(1, 1, 2, 66, 2, 50, 628),
        Booking(1, 1, 0, 66, 2, 50, 628),
        Booking(1, 1, 1, 66, 2, 50, 675)
      ))

      When("finding most popular booked hotels with same country")
      val result = Task2HotelsApplication.findMostPopularHotelBookedAndSearchedFromSameCountry(spark, dataset)

      Then("expect to get an empty dataset")
      assert(result.count == 0)
    }

    scenario("Calculates counts correctly by grouping by country and filtering out non-booked and not withing same country") {
      import spark.implicits._

      Given("Dataset with bookings made by singles only")
      val dataset = spark.createDataset(Seq(
        Booking(1, 2, 2, 50, 2, 50, 628),
        Booking(1, 2, 0, 66, 2, 50, 628), // not the same country
        Booking(1, 2, 0, 50, 2, 50, 628),
        Booking(1, 2, 1, 50, 2, 50, 675),
        Booking(0, 1, 1, 50, 2, 50, 675), // not booked
        Booking(1, 2, 0, 66, 2, 66, 628),
        Booking(1, 2, 1, 66, 2, 66, 675)
      ))

      When("finding most popular booked hotels with same country")
      val result = Task2HotelsApplication.findMostPopularHotelBookedAndSearchedFromSameCountry(spark, dataset)

      Then("expect to get a dataset of 1 entry with 3 bookings")
      assert(result.count == 1)
      assert(result.first == (50, 3))
    }
  }

  feature("Outputting to multiple outputs") {

    scenario("Outputting to console and specified csv file") {
      import spark.implicits._

      Given("Resulting dataset of hotels and their popularity and target csv path")
      val dataset = spark.createDataset(Seq(
        (50, 2L),
        (66, 1L)
      ))
      val path = "target/task-2-test-output/" + UUID.randomUUID().toString + ".csv"

      When("calling output function")
      Task2HotelsApplication.outputResult(spark, dataset, path)

      Then("expect the file to be written with proper content")
      val file = File(path).toDirectory.files.filter(_.extension == "csv").toSeq.head
      val lines = Source.fromFile(file.path).getLines.toSeq
      assert(lines == Seq("50,2", "66,1"))
    }
  }

}
