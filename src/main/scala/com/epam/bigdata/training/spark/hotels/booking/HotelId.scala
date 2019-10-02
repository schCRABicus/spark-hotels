package com.epam.bigdata.training.spark.hotels.booking

/**
  * Hotel id interface.
  * Provides composite id consisting of hotel continent, country and market.
  */
trait HotelId {
  def hotel_continent: Int
  def hotel_country: Int
  def hotel_market: Int

  /**
    * Composite hotel id.
    * @return
    */
  def hotel_id: CompositeHotelId = CompositeHotelId(hotel_continent, hotel_country, hotel_market)
}

case class CompositeHotelId(hotel_continent: Int, hotel_country: Int, hotel_market: Int)


