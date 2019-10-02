package com.epam.bigdata.training.spark.hotels.booking

/**
  * Represents a booking domain model with only the fields being used in the application.
  * The other columns are omitted as they are not used in computations.
  *
  * @param is_booking             1 if hotel booked successfully and 0 otherwise.
  * @param srch_adults_cnt        Number of adults booking the hotel
  * @param srch_children_cnt      Number of children booking the hotel
  * @param user_location_country  User search location (country)
  * @param hotel_continent        Hotel continent
  * @param hotel_country          Hotel country
  * @param hotel_market           Hotel market
  */
case class RawBooking(/*is_booking: String, */srch_adults_cnt:String, srch_children_cnt: String, user_location_country: String, hotel_continent: String, hotel_country: String, hotel_market: String) {

  def asBooking(): Booking = {
    Booking(
//      is_booking.toInt,
      1,
      srch_adults_cnt.toInt,
      srch_children_cnt.toInt,
      user_location_country.toInt,
      hotel_continent.toInt,
      hotel_country.toInt,
      hotel_market.toInt
    )
  }
}
