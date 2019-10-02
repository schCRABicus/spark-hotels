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
case class Booking(is_booking: Int, srch_adults_cnt:Int, srch_children_cnt: Int, user_location_country: Int, hotel_continent: Int, hotel_country: Int, hotel_market: Int) extends HotelId {

  /**
    * Whether the booking was successful or not.
    * @return True if booked and false otherwise
    */
  def booked: Boolean = is_booking == 1
}
