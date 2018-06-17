package de.uni_stuttgart.plugwise

import org.joda.time.DateTime

trait PlugwiseProtocol extends PlugwiseUtils {

  def parsePacket(rawPacket: String): Any = {
    val responseCode   = rawPacket.substring(0,4)
    val sequenceNumber = rawPacket.substring(4,8)

    debug(s"Response Code: $responseCode, Sequence Number: $sequenceNumber, Parsing Packet: ${rawPacket.substring(0,rawPacket.length-4)}")

    responseCode match {
      case "0000" => // ACK / NACK
        val acknowledgeCode = rawPacket.substring(8,12)
        acknowledgeCode match {
          case AckCodes.StickAck =>
            debug(s"STICK ACK Sequence Number: $sequenceNumber")
          case AckCodes.StickNack =>
            warn(s"STICK NACK Sequence Number: $sequenceNumber")
            throw new Exception(s"STICK NACK Sequence Number: $sequenceNumber")

          case AckCodes.CircleAckClock =>
            debug(s"CIRCLE ACK SET CLOCK Sequence Number: $sequenceNumber")
            val circleMac = rawPacket.substring(12,28)
            ResponseResults.CircleSetClock(circleMac)
          case AckCodes.CircleAckOn =>
            debug(s"CIRCLE ACK ON Sequence Number: $sequenceNumber")
            val circleMac = rawPacket.substring(12,28)
            ResponseResults.CircleOn(circleMac)
          case AckCodes.CircleAckOff =>
            debug(s"CIRCLE ACK OFF Sequence Number: $sequenceNumber")
            val circleMac = rawPacket.substring(12,28)
            ResponseResults.CircleOff(circleMac)
          case AckCodes.CircleAckInterval =>
            debug(s"CIRCLE ACK INTERVAL Sequence Number: $sequenceNumber")
            val circleMac = rawPacket.substring(12,28)
            ResponseResults.CircleInterval(circleMac)
          case AckCodes.CircleNack =>
            warn(s"CIRCLE NACK NOT FOUND Sequence Number: $sequenceNumber")
            throw new Exception(s"STICK DEVICE NOT FOUND Sequence Number: $sequenceNumber")

          case _ =>
            error(s"Unknown acknowledge code: $acknowledgeCode")
            throw new Exception(s"Unknown acknowledge code: $acknowledgeCode")
        }

      case "0011" => // STICK INIT
        val stickMac            = rawPacket.substring(8,24)
        val circlePlusConnected = if(rawPacket.substring(26,28) == "01") true else false
        val networkMac          = rawPacket.substring(28,44)
        val networkShortMac     = rawPacket.substring(44,48)

        debug(s"STICK INIT Mac: $stickMac Network: $networkMac ($networkShortMac) Connected: $circlePlusConnected")
        ResponseResults.StickInit(stickMac, circlePlusConnected, networkMac, networkShortMac)

      case "0013" => // CIRCLE POWER
        val circleMac    = rawPacket.substring(8,24)
        val pulses1Sec   = hexToShort(rawPacket.substring(24,28))
        val pulses8Sec   = hexToShort(rawPacket.substring(28,32))
        val pulses1h     = hexToInt(rawPacket.substring(32,40))
        val pulsesProd1h = hexToInt(rawPacket.substring(40,48))

        debug(s"CIRCLE POWER Mac: $circleMac Pulses 1s: ${rawPacket.substring(24,28)} Pulses 8s: ${rawPacket.substring(28,32)} Pulses 1h: ${rawPacket.substring(32,40)} Pulses Produced 1h: ${rawPacket.substring(40,48)}")
        debug(s"CIRCLE POWER Mac: $circleMac Pulses 1s: $pulses1Sec Pulses 8s: $pulses8Sec Pulses 1h: $pulses1h Pulses Produced 1h: $pulsesProd1h")
        ResponseResults.CirclePower(circleMac, pulses1Sec, pulses8Sec, pulses1h, pulsesProd1h)

      case "0024" => // CIRCLE INFO
        val circleMac       = rawPacket.substring(8,24)
        val year            = hexToInt(rawPacket.substring(24,26))
        val month           = hexToInt(rawPacket.substring(26,28))
        val rawMinutes      = hexToInt(rawPacket.substring(28,32))
        val logAddress      = logToInt(rawPacket.substring(32,40))
        val state           = if(rawPacket.substring(40,42) == "01") true else false
        val rawHertz        = hexToInt(rawPacket.substring(42,44))
        val hardwareVersion = rawPacket.substring(44,56)
        val firmwareVersion = rawPacket.substring(56,64)

        val hertz = if(rawHertz == 113) 50 else 60
        val dateTime = rawMinutesToDateTime(year, month, rawMinutes)

        debug(s"CIRCLE INFO Mac: $circleMac DateTime: $dateTime Log Address: $logAddress State: $state Hertz: $hertz Hardware Version: $hardwareVersion Firmware Version: $firmwareVersion")
        ResponseResults.CircleInfo(circleMac, dateTime, logAddress, state, hertz, hardwareVersion, firmwareVersion)

      case "0027" => // CIRCLE CALIBRATION
        val circleMac   = rawPacket.substring(8,24)
        val gainA       = hexToFloat(rawPacket.substring(24,32))
        val gainB       = hexToFloat(rawPacket.substring(32,40))
        val offsetTotal = hexToFloat(rawPacket.substring(40,48))
        val offsetNoise = hexToFloat(rawPacket.substring(48,56))

        debug(s"CIRCLE CALIBRATION Mac: $circleMac GainA: $gainA GainB: $gainB Offset Total: $offsetTotal Offset Noise: $offsetNoise")
        ResponseResults.CircleCalibration(circleMac, gainA, gainB, offsetTotal, offsetNoise)

      case "003F" => // CIRCLE CLOCK INFO
        val circleMac = rawPacket.substring(8,24)
        val hours     = hexToInt(rawPacket.substring(24,26))
        val minutes   = hexToInt(rawPacket.substring(26,28))
        val seconds   = hexToInt(rawPacket.substring(28,30))
        val dayOfWeek = hexToInt(rawPacket.substring(30,32))

        debug(s"CIRCLE CLOCK INFO Mac: $circleMac Hours: $hours Minutes: $minutes Seconds: $seconds Day Of Week: $dayOfWeek")

        ResponseResults.CircleClockInfo(circleMac, hours, minutes, seconds, dayOfWeek)

      case "0049" => // CIRCLE BUFFER
        val circleMac  = rawPacket.substring(8,24)
        val log1Date   = logDateToDateTime(rawPacket.substring(24, 32))
        val log1Pulses = hexToInt(rawPacket.substring(32, 40))
        val log2Date   = logDateToDateTime(rawPacket.substring(40, 48))
        val log2Pulses = hexToInt(rawPacket.substring(48, 56))
        val log3Date   = logDateToDateTime(rawPacket.substring(56, 64))
        val log3Pulses = hexToInt(rawPacket.substring(64, 72))
        val log4Date   = logDateToDateTime(rawPacket.substring(72, 80))
        val log4Pulses = hexToInt(rawPacket.substring(80, 88))
        val logAddress = rawPacket.substring(88, 96)

        debug(s"CIRCLE BUFFER Mac: $circleMac Date: $log1Date Pulses: ${rawPacket.substring(32, 40)} Date: $log2Date Pulses: ${rawPacket.substring(48, 56)} Date: $log3Date Pulses: ${rawPacket.substring(64, 72)} Date: $log4Date Pulses: ${rawPacket.substring(80, 88)} Log Address: $logAddress")
        debug(s"CIRCLE BUFFER Mac: $circleMac Date: $log1Date Pulses: $log1Pulses Date: $log2Date Pulses: $log2Pulses Date: $log3Date Pulses: $log3Pulses Date: $log4Date Pulses: $log4Pulses Log Address: $logAddress")
        ResponseResults.CircleBuffer(circleMac, log1Date, log1Pulses, log2Date, log2Pulses, log3Date, log3Pulses, log4Date, log4Pulses, logAddress)
      case _ =>
        error(s"Unknown response code: $responseCode ($rawPacket)")
        throw new Exception(s"Unknown response code: $responseCode ($rawPacket)")
    }
  }

  def isStickAck(responsePacket: ResponsePacket): Boolean = {
    responsePacket.rawPacket.startsWith("0000") && responsePacket.rawPacket.substring(8,12) == "00C1"
  }
}

object Packets {
  final val Start = "\u0005\u0005\u0003\u0003"
  final val End   = "\r\n"
}

object RequestCodes {
  final val StickInit = "000A"

  final val CircleOnOff       = "0017"
  final val CircleInfo        = "0023"
  final val CirclePower       = "0012"
  final val CircleSetClock    = "0016"
  final val CircleCalibration = "0026"
  final val CircleClockInfo   = "003E"

  final val CircleInterval = "0057"
  final val CircleBuffer   = "0048"

  final val CircleOn  = "01"
  final val CircleOff = "00"
}

object ResponseCodes {
  final val Ack = "0000"

  final val StickInit       = "0011"
  final val StickInitFailed = "00C2"

  final val CirclePower       = "0013"
  final val CircleInfo        = "0024"
  final val CircleCalibration = "0027"
  final val CircleClockInfo  = "003F"
}

object AckCodes {
  final val StickAck  = "00C1"
  final val StickNack = "00E1"

  final val CircleAckClock    = "00D7"
  final val CircleAckOn       = "00D8"
  final val CircleAckOff      = "00DE"
  final val CircleAckInterval = "00F8"

  final val CircleNack = "00C2"
}

object ResponseResults {
  case class StickInit(stickMac: String, circlePlusConnected: Boolean, networkMac: String, networkShortMac: String)

  case class CircleOn (circleMac: String)
  case class CircleOff(circleMac: String)

  case class CircleInfo       (circleMac: String, dateTime: DateTime, logAddress: Int, deviceState: Boolean, hertz: Int, hardwareVersion: String, firmwareVersion: String)
  case class CirclePower      (circleMac: String, pulses1s: Float, pulses8s: Float, pulses1h: Float, pulsesProd1h: Float)
  case class CircleCalibration(circleMac: String, gainA: Float, gainB: Float, offsetTotal: Float, offsetNoise: Float)

  case class CircleBuffer  (circleMac: String, log1Date: Option[DateTime], log1Pulses: Float, log2Date: Option[DateTime], log2Pulses: Float, log3Date: Option[DateTime], log3Pulses: Float, log4Date: Option[DateTime], log4Pulses: Float, logAddress: String)
  case class CircleInterval(circleMac: String)

  case class CircleConsumption          (circleMac: String, watt1s: Float, watt8s: Float, watt1h: Float, wattProd1h: Float)
  case class CircleHistoricalConsumption(circleMac: String, dateTime: DateTime, wattHours: Float, watt: Float, wattHoursProd: Float, wattProd: Float)

  case class CircleClockInfo(circleMac: String, hours: Int, minutes: Int, seconds: Int, dayOfWeek: Int)
  case class CircleSetClock (circleMac: String)
}