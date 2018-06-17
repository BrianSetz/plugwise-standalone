package de.uni_stuttgart.plugwise.main

import de.uni_stuttgart.plugwise.Plugwise
import grizzled.slf4j.Logging

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object Main extends App with Logging {
  val plugwise = new Plugwise("ttyUSB0")

  val openFuture = plugwise.open()

  openFuture.onComplete {
    case Success(result) =>
      info("Plugwise USB connection opened successfully")

      plugwise.circleOn("AABBCCDDEEFF")
      plugwise.circleOff("AABBCCDDEEFF")
    case Failure(exception) =>
      error("Failed to open Plugwise USB connection", exception)
  }
}
