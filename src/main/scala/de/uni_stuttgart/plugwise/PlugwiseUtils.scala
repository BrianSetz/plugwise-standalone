package de.uni_stuttgart.plugwise

import grizzled.slf4j.Logging
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.mutable
import scala.collection.JavaConverters._

trait PlugwiseUtils extends Logging {
  val PULSES_PER_KILOWATT_SECOND = 468.9385193f

  def crc(string: String): String = {
    var crc        = 0x0000 // initial value
    val polynomial = 0x1021 // 0001 0000 0010 0001  (0, 5, 12)

    val bytes = string.getBytes("ASCII")

    for {
      b <- bytes
      i <- 0 until 8
    } {
      val bit = (b >> (7 - i) & 1) == 1
      val c15 = (crc >> 15 & 1) == 1
      crc <<= 1
      if (c15 ^ bit) crc ^= polynomial
    }
    crc &= 0xffff
    "%04x".format(crc)
  }

  def hexToInt(hex: String): Int = BigInt(hex, 16).toInt
  def hexToShort(hex: String): Short = hexToInt(hex).toShort

  def intToHex(x: Int, size: Int): String = s"%0${size}x".format(x).toUpperCase

  def hexToFloat(hex: String): Float = java.lang.Float.intBitsToFloat(java.lang.Long.parseLong(hex, 16).toInt)

  def logToInt(hex: String) : Int = (hexToInt(hex) - 278528) / 32
  def intToLog(int: Int) : String = intToHex((int * 32) + 278528, 8)

  def rawMinutesToDateTime(year: Int, month: Int, rawMinutes: Int): DateTime = {
    val days = rawMinutes / (60 * 24)
    val hours = (rawMinutes - (days * 60 * 24)) / 60
    val minutes = rawMinutes - (days * 60 * 24) - (hours * 60)

    new DateTime(new DateTime(year + 2000, month, days + 1, hours, minutes, 0, 0, DateTimeZone.UTC))
  }

  def dateTimeToRaw(dateTime: DateTime): (Int, Int, Int, Int, Int, Int, Int) = {
    val utcDateTime = new DateTime(dateTime, DateTimeZone.UTC)

    val year = utcDateTime.getYear - 2000
    val month = utcDateTime.getMonthOfYear
    val totalMinutes = (utcDateTime.dayOfMonth().get()-1)*24*60 + utcDateTime.getHourOfDay*60 + utcDateTime.getMinuteOfHour

    val hours   = utcDateTime.getHourOfDay
    val minutes = utcDateTime.getMinuteOfHour
    val seconds = utcDateTime.getSecondOfMinute

    val dayOfWeek = utcDateTime.dayOfWeek().get()

    (year, month, totalMinutes, hours, minutes, seconds, dayOfWeek)
  }

  def logDateToDateTime(logDate: String): Option[DateTime] = {
    val year = hexToInt(logDate.substring(0, 2))
    val month = hexToInt(logDate.substring(2, 4))
    val rawMinutes = hexToInt(logDate.substring(4, 8))

    if(month > 12)
      None
    else
      Some(rawMinutesToDateTime(year, month, rawMinutes))
  }

  def bufferToList(buffer: ResponseResults.CircleBuffer): List[(DateTime, Float)] = {
    val list = mutable.ListBuffer[(DateTime, Float)]()

    buffer.log1Date.map(dt => list += ((dt, buffer.log1Pulses)))
    buffer.log2Date.map(dt => list += ((dt, buffer.log2Pulses)))
    buffer.log3Date.map(dt => list += ((dt, buffer.log3Pulses)))
    buffer.log4Date.map(dt => list += ((dt, buffer.log4Pulses)))

    list.toList
  }

  def pulsesToWatt(pulses: Float): Float = (pulses / PULSES_PER_KILOWATT_SECOND) * 1000.0f

  def correctPulses(pulses: Float, seconds: Int, gainA: Float, gainB: Float, offsetTotal: Float, offsetNoise: Float): Float = {
    if(pulses == 0.0f)
      return 0.0f

    val pulsesPerSecond = pulses / seconds.toFloat
    val correctedPulses = seconds.toFloat * (((Math.pow(pulsesPerSecond + offsetNoise,2.0) * gainB) + ((pulsesPerSecond + offsetNoise) * gainA)) + offsetTotal)

    if (pulses > 0.0f && correctedPulses < 0.0f || pulses < 0.0f && correctedPulses > 0.0f)
      0
    else
      correctedPulses.toFloat
  }

  def correctPulses(pulses: Float, seconds: Int, calibration: ResponseResults.CircleCalibration): Float =
    correctPulses(pulses, seconds, calibration.gainA, calibration.gainB, calibration.offsetTotal, calibration.offsetNoise)

  def correctPulses(calibration: ResponseResults.CircleCalibration, power: ResponseResults.CirclePower): ResponseResults.CirclePower = {
    val now = new DateTime()
    val pulses1sCorrected = correctPulses(power.pulses1s, 1, calibration.gainA, calibration.gainB, calibration.offsetTotal, calibration.offsetNoise)
    val pulses8sCorrected = correctPulses(power.pulses8s, 8, calibration.gainA, calibration.gainB, calibration.offsetTotal, calibration.offsetNoise)
    val pulses1hCorrected = correctPulses(power.pulses1h, now.getMinuteOfHour * 60 + now.getSecondOfMinute, calibration.gainA, calibration.gainB, calibration.offsetTotal, calibration.offsetNoise)
    val pulsesProd1hCorrected = correctPulses(power.pulsesProd1h, 3600, calibration.gainA, calibration.gainB, calibration.offsetTotal, calibration.offsetNoise)

    debug(s"Pulses 1s: ${power.pulses1s} Corrected: $pulses1sCorrected")
    debug(s"Pulses 8s: ${power.pulses8s} Corrected: $pulses8sCorrected")
    debug(s"Pulses 1h: ${power.pulses1h} Corrected: $pulses1hCorrected")
    debug(s"Pulses Produced 1h: ${power.pulsesProd1h} Corrected: $pulsesProd1hCorrected")

    ResponseResults.CirclePower(power.circleMac, pulses1sCorrected, pulses8sCorrected, pulses1hCorrected, pulsesProd1hCorrected)
  }

  def correctPulses(calibration: ResponseResults.CircleCalibration, buffer: ResponseResults.CircleBuffer, bufferInterval: Int): ResponseResults.CircleBuffer = {
    val log1PulsesCorrected = if(buffer.log1Date.isDefined) correctPulses(buffer.log1Pulses, bufferInterval, calibration) else -1
    val log2PulsesCorrected = if(buffer.log2Date.isDefined) correctPulses(buffer.log2Pulses, bufferInterval, calibration) else -1
    val log3PulsesCorrected = if(buffer.log3Date.isDefined) correctPulses(buffer.log3Pulses, bufferInterval, calibration) else -1
    val log4PulsesCorrected = if(buffer.log4Date.isDefined) correctPulses(buffer.log4Pulses, bufferInterval, calibration) else -1

    ResponseResults.CircleBuffer(buffer.circleMac, buffer.log1Date, log1PulsesCorrected, buffer.log2Date, log2PulsesCorrected, buffer.log3Date, log3PulsesCorrected, buffer.log4Date, log4PulsesCorrected, buffer.logAddress)
  }
}