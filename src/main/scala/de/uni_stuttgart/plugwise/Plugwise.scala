package de.uni_stuttgart.plugwise

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, ClosedShape}
import akka.stream.scaladsl._
import akka.util.ByteString
import de.uni_stuttgart.reactive.Reactive
import de.uni_stuttgart.serial.Serial
import grizzled.slf4j.Logging
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, TimeoutException}
import scala.util.{Failure, Success}

trait PlugwiseSettings extends Logging {
  val throttle = new FiniteDuration(Duration("100 milliseconds").toMillis, TimeUnit.MILLISECONDS)
  val timeout = new FiniteDuration(Duration("5 minutes").toMillis, TimeUnit.MILLISECONDS)
}

trait PlugwiseApi extends PlugwiseSettings {
  def open(): Future[ResponseResults.StickInit]

  def stickInit(): Future[ResponseResults.StickInit]

  def circleOn(mac: String)  : Future[ResponseResults.CircleOn]
  def circleOff(mac: String) : Future[ResponseResults.CircleOff]
  def circleInfo(mac: String): Future[ResponseResults.CircleInfo]

  def circleClockInfo(mac: String)                   : Future[ResponseResults.CircleClockInfo]
  def circleSetClock(mac: String, dateTime: DateTime): Future[ResponseResults.CircleSetClock]

  def circleCalibration(mac: String): Future[ResponseResults.CircleCalibration]
  def circlePower(mac: String)      : Future[ResponseResults.CirclePower]

  def circleInterval(mac: String, consumptionInterval: Int, productionInterval: Int): Future[ResponseResults.CircleInterval]
  def circleBuffer(mac: String, logAddress: Int)                                    : Future[ResponseResults.CircleBuffer]

  def circleConsumption(mac: String)                                                : Future[ResponseResults.CircleConsumption]
  def circleConsumption(mac: String, calibration: ResponseResults.CircleCalibration): Future[ResponseResults.CircleConsumption]

  def circleHistoricalConsumption(mac: String, bufferInterval: Int): Future[ResponseResults.CircleHistoricalConsumption]
  def circleHistoricalConsumption(mac: String, calibration: ResponseResults.CircleCalibration, bufferInterval: Int): Future[ResponseResults.CircleHistoricalConsumption]
}

case class RequestPacket(rawPacket: String, stickResponse: Promise[ResponsePacket] = Promise[ResponsePacket], circleResponse: Promise[ResponsePacket] = Promise[ResponsePacket])
case class ResponsePacket(responseCode: String, sequenceNumber: String, rawPacket: String)

class Plugwise(comPort: String) extends PlugwiseApi with PlugwiseReactive with PlugwiseProtocol with PlugwiseUtils {
  implicit val system: ActorSystem = ActorSystem("plugwise-actor-system")
  implicit val mat: ActorMaterializer = ActorMaterializer(ActorMaterializerSettings(system).withSupervisionStrategy(supervisor))
  implicit val executionContext: ExecutionContext = system.dispatcher

  val pendingPackets = new mutable.HashMap[String, RequestPacket]
  val (pushSource, stick) = Reactive.pushSource[RequestPacket]()

  override def open(): Future[ResponseResults.StickInit] = {
    val openTry = Serial.open(comPort, 115200, 8, 1, 0, 0, 1, 400)

    if(openTry.isFailure)
      return Future.failed[ResponseResults.StickInit](openTry.failed.get)

    val (serialSource, serialSink) = openTry.get

    val streamGraph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val frameDelimiter = "\r\n"
      val maxFrameLength = 4096

      val responsePacketSource = serialSource
        .via(Framing.delimiter(ByteString(frameDelimiter), maximumFrameLength = maxFrameLength, allowTruncation = true))
        .via(cleanPacket)
        .via(printOutput("Received:", trace(_)))
        .via(byteStringToResponse)


      val packetSink = pushSource
        .via(throttle(throttle))

      val zipRequestStickResponse = builder.add(Zip[RequestPacket, ResponsePacket])
      val broadcastRequest = builder.add(Broadcast[RequestPacket](2))
      val broadcastResponse = builder.add(Broadcast[ResponsePacket](2))

      packetSink ~> broadcastRequest.in
      broadcastRequest.out(0) ~> makePacket ~> printOutput("Sent:", trace(_)) ~> serialSink
      broadcastRequest.out(1) ~> zipRequestStickResponse.in0

      responsePacketSource ~> broadcastResponse.in
      broadcastResponse.out(0).filter(isStickAck) ~> zipRequestStickResponse.in1
      broadcastResponse.out(1).filter(!isStickAck(_)).map(completeRequest) ~> Sink.ignore

      zipRequestStickResponse.out.map { case (request, stickResponse) =>
        pendingPackets += stickResponse.sequenceNumber -> request
        request.stickResponse trySuccess stickResponse
      } ~> Sink.ignore

      ClosedShape
    })

    streamGraph.run()
    stickInit()
  }

  private def completeRequest(circleResponse: ResponsePacket): Unit = {
    val sequenceNumber = circleResponse.sequenceNumber
    trace(s"Sequence Number: $sequenceNumber received")

    if(pendingPackets.contains(sequenceNumber)) {
      val request = pendingPackets(sequenceNumber)

      request.stickResponse.future.map{ _ =>
        if(request.circleResponse.isCompleted)
          warn(s"Sequence Number: $sequenceNumber already completed")
        else
          request.circleResponse trySuccess circleResponse
      }

      pendingPackets.remove(sequenceNumber)
    } else {
      error(s"Sequence Number: $sequenceNumber not found for response: ${circleResponse.rawPacket}")
    }
  }

  private def sendRequest(request: String): RequestPacket = {
    val requestPacket = RequestPacket(request)

    requestPacket.stickResponse.future onComplete {
      case Success(stickResponse) =>
        trace(s"Sequence Number: ${stickResponse.sequenceNumber} pending for request ${requestPacket.rawPacket}")

        requestPacket.circleResponse.future onComplete {
          case Success(circleResponse) =>
            debug(s"Sequence Number: ${circleResponse.sequenceNumber} completed")
          case Failure(exception) =>
            warn(s"Did not get response for ${stickResponse.sequenceNumber} ${requestPacket.rawPacket}: ${exception.getMessage}")
            pendingPackets.remove(stickResponse.sequenceNumber)
        }
      case Failure(exception) =>
        warn(s"Did not get response from stick for request ${requestPacket.rawPacket}: ${exception.getMessage}")
    }

    def promiseTimeout[T](timeout: FiniteDuration, promise: Promise[T]): Unit = {
      system.scheduler.scheduleOnce(timeout){ promise tryFailure new TimeoutException(s"Promise timed out after $timeout") }
    }

    promiseTimeout(timeout, requestPacket.stickResponse) // TODO: start timeout when request is send
    promiseTimeout(timeout, requestPacket.circleResponse) // TODO: start timeout when request is send

    stick(requestPacket)

    requestPacket
  }

  override def stickInit(): Future[ResponseResults.StickInit] = {
    sendRequest(RequestCodes.StickInit)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.StickInit])
  }

  override def circleOn(mac: String): Future[ResponseResults.CircleOn] = {
    debug(s"CIRCLE ON Mac: $mac")
    sendRequest(RequestCodes.CircleOnOff + mac + RequestCodes.CircleOn)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleOn])
  }

  override def circleOff(mac: String): Future[ResponseResults.CircleOff] = {
    debug(s"CIRCLE OFF Mac: $mac")
    sendRequest(RequestCodes.CircleOnOff + mac + RequestCodes.CircleOff)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleOff])
  }

  override def circleInfo(mac: String): Future[ResponseResults.CircleInfo] = {
    debug(s"CIRCLE INFO Mac: $mac")
    sendRequest(RequestCodes.CircleInfo + mac)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleInfo])
  }

  override def circleClockInfo(mac: String): Future[ResponseResults.CircleClockInfo] = {
    debug(s"CIRCLE CLOCK INFO Mac: $mac")
    sendRequest(RequestCodes.CircleClockInfo + mac)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleClockInfo])
  }

  override def circleSetClock(mac: String, dateTime: DateTime): Future[ResponseResults.CircleSetClock] = {
    val utcDateTime = new DateTime(dateTime, DateTimeZone.UTC)
    debug(s"CIRCLE SET CLOCK Mac: $mac DateTime: $utcDateTime")

    val logAddress = "FFFFFFFF"
    val (year, month, totalMinutes, hours, minutes, seconds, dayOfWeek) = dateTimeToRaw(utcDateTime)

    sendRequest(RequestCodes.CircleSetClock + mac + intToHex(year, 2) + intToHex(month, 2) + intToHex(totalMinutes, 4) + logAddress + intToHex(hours, 2) + intToHex(minutes, 2) + intToHex(seconds, 2) + intToHex(dayOfWeek, 2))
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleSetClock])
  }

  override def circleCalibration(mac: String): Future[ResponseResults.CircleCalibration] = {
    debug(s"CIRCLE CALIBRATION Mac: $mac")
    sendRequest(RequestCodes.CircleCalibration + mac)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleCalibration])
  }

  override def circlePower(mac: String): Future[ResponseResults.CirclePower] = {
    debug(s"CIRCLE POWER Mac: $mac")
    sendRequest(RequestCodes.CirclePower + mac)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CirclePower])
  }

  override def circleInterval(mac: String, consumptionInterval: Int, productionInterval: Int): Future[ResponseResults.CircleInterval] = {
    debug(s"CIRCLE INTERVAL Mac: $mac Consumption: $consumptionInterval minutes, Production: $productionInterval minutes")
    sendRequest(RequestCodes.CircleInterval + mac + intToHex(consumptionInterval,4) + intToHex(productionInterval, 4))
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleInterval])
  }

  override def circleBuffer(mac: String, logAddress: Int): Future[ResponseResults.CircleBuffer] = {
    debug(s"CIRCLE BUFFER Mac: $mac, Log Address: $logAddress")
    if(logAddress < 0 )
      return Future { ResponseResults.CircleBuffer(mac, None, -1, None, -1, None, -1, None, -1, intToLog(logAddress))}

    val logAddressHex = intToLog(logAddress)

    sendRequest(RequestCodes.CircleBuffer + mac + logAddressHex)
      .circleResponse.future.map(x => parsePacket(x.rawPacket).asInstanceOf[ResponseResults.CircleBuffer])
  }

  override def circleConsumption(mac: String): Future[ResponseResults.CircleConsumption] = {
    for {
      calibration <- circleCalibration(mac)
      consumption <- circleConsumption(mac, calibration)
    } yield {
      consumption
    }
  }

  override def circleConsumption(mac: String, calibration: ResponseResults.CircleCalibration): Future[ResponseResults.CircleConsumption] = {
    for {
      power <- circlePower(mac)
    } yield {
      val correctedPulses = correctPulses(calibration, power)

      debug(s"CIRCLE CONSUMPTION Mac: $mac Watt: ${pulsesToWatt(correctedPulses.pulses1s)}")
      ResponseResults.CircleConsumption(mac, pulsesToWatt(correctedPulses.pulses1s), pulsesToWatt(correctedPulses.pulses8s), pulsesToWatt(correctedPulses.pulses1h), pulsesToWatt(correctedPulses.pulsesProd1h))
    }
  }

  override def circleHistoricalConsumption(mac: String, bufferInterval: Int): Future[ResponseResults.CircleHistoricalConsumption] = {
    for {
      calibration           <- circleCalibration(mac)
      historicalConsumption <- circleHistoricalConsumption(mac, calibration, bufferInterval)
    } yield {
      historicalConsumption
    }
  }

  override def circleHistoricalConsumption(mac: String, calibration: ResponseResults.CircleCalibration, bufferInterval: Int): Future[ResponseResults.CircleHistoricalConsumption] = {
    for {
      cInfo          <- circleInfo(mac)
      bufferPrevious <- circleBuffer(mac, cInfo.logAddress - 1)
      bufferCurrent  <- circleBuffer(mac, cInfo.logAddress)
    } yield {
      val bufferCurrentCorrected  = correctPulses(calibration, bufferCurrent, bufferInterval)
      val bufferPreviousCorrected = correctPulses(calibration, bufferPrevious, bufferInterval)

      // TODO: sortedWattHour can contain duplicate timestamps, in that case one signifies production (negative) and the other consumption (positive)
      val combined = bufferToList(bufferCurrentCorrected) ++ bufferToList(bufferPreviousCorrected)

      case class WattData(wattHour: Float, watt: Float)
      val sortedWattHour = combined
        .map    { case(dt, pulses) => dt -> WattData(pulsesToWatt(pulses) / 3600, pulsesToWatt(pulses) / bufferInterval) }
        .sortBy { case(dt, _)      => dt.getMillis }

      val consProd = sortedWattHour
        .groupBy  { case (dt, _) => dt }.toSeq
        .maxBy    { case (dt, _) => dt.getMillis }._2
        .sortWith { case((_, data1), (_, data2)) => data1.watt > data2.watt }

      val result = if(consProd.length <= 1)
        ResponseResults.CircleHistoricalConsumption(mac, consProd.head._1, consProd.head._2.wattHour, consProd.head._2.watt, 0, 0)
      else
        ResponseResults.CircleHistoricalConsumption(mac, consProd.head._1, consProd.head._2.wattHour, consProd.head._2.watt, consProd.last._2.wattHour, consProd.last._2.watt)

      debug(s"CIRCLE HISTORICAL CONSUMPTION Mac: $mac DateTime: ${result.dateTime} Watt hour: ${result.wattHours}} Watt: ${result.watt} Watt hour prod: ${result.wattHoursProd}} Watt prod: ${result.wattProd}")

      result
    }
  }
}