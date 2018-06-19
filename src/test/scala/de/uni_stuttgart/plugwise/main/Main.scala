package de.uni_stuttgart.plugwise.main

import de.uni_stuttgart.plugwise.Plugwise
import grizzled.slf4j.Logging

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App with Logging {
  val plugwise = new Plugwise("COM3")

  val openFuture = plugwise.open()

  val mac = "000D6F0004B1E8C0"

  openFuture.onComplete {
    case Success(result) =>
      info("Plugwise USB connection opened successfully")

      plugwise.circleConsumption(mac).onComplete {
        case Success(consumption) => info(s"Current electricity consumption in W: ${consumption.watt1s}")
        case Failure(e) => error(e)
      }

      plugwise.circleOff(mac).onComplete {
        case Success(r) => info("Circles has been turned off.")
        case Failure(e) => error(e)
      }
    case Failure(exception) =>
      error("Failed to open Plugwise USB connection", exception)
  }
}
