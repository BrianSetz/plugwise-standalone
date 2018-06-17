package de.uni_stuttgart.serial

import java.io.{InputStream, OutputStream}

import akka.stream.IOResult
import akka.stream.scaladsl.{Sink, Source}
import akka.util.ByteString
import com.fazecast.jSerialComm.SerialPort
import de.uni_stuttgart.reactive.Reactive
import grizzled.slf4j.Logging

import scala.concurrent.Future
import scala.util.Try

trait SerialSettings {
  val ns = "rugds.serial"

//  val comPort: String = config.getString(s"$ns.com-port")
//  val baudRate: Int = config.getInt(s"$ns.baud-rate")
//  val dataBits: Int = config.getInt(s"$ns.data-bits")
//  val stopBits: Int = config.getInt(s"$ns.stop-bits")
//  val parity: Int = config.getInt(s"$ns.parity")
//
//  val flowControl: Int = config.getInt(s"$ns.flow-control")
//  val timeoutMode: Int = config.getInt(s"$ns.timeout-mode")
//  val timeout: Int = config.getInt(s"$ns.timeout")
}

trait SerialApi extends Logging {
  def open(comPort:     String,
           baudRate:    Int,
           dataBits:    Int,
           stopBits:    Int,
           parity:      Int,
           flowControl: Int,
           timeoutMode: Int,
           timeout:     Int): Try[(Source[ByteString, Future[IOResult]], Sink[ByteString, Future[IOResult]])]

  def openIO(comPort:     String,
             baudRate:    Int,
             dataBits:    Int,
             stopBits:    Int,
             parity:      Int,
             flowControl: Int,
             timeoutMode: Int,
             timeout:     Int): (InputStream, OutputStream)

  def listSerialPorts()
}


object Serial extends SerialApi {
  override def listSerialPorts() {
    info("Listing serial ports:")
    SerialPort.getCommPorts.foreach { serialPort =>
      info(s"Name: ${serialPort.getSystemPortName} - Description: ${serialPort.getDescriptivePortName} - isOpen: ${serialPort.isOpen}")
    }
  }

  override def open(comPort:     String,
           baudRate:    Int,
           dataBits:    Int,
           stopBits:    Int,
           parity:      Int,
           flowControl: Int,
           timeoutMode: Int,
           timeout: Int): Try[(Source[ByteString, Future[IOResult]], Sink[ByteString, Future[IOResult]])] =
  {
    Try(openIO(comPort, baudRate, dataBits, stopBits, parity, flowControl, timeoutMode, timeout))
      .map { case(in, out) => (Reactive.streamSource(in), Reactive.streamSink(out)) }
  }

  override def openIO(comPort:     String,
             baudRate:    Int,
             dataBits:    Int,
             stopBits:    Int,
             parity:      Int,
             flowControl: Int,
             timeoutMode: Int,
             timeout: Int): (InputStream, OutputStream) = {
    info(s"Opening serial port. comPort: $comPort, baudRate: $baudRate, dataBits: $dataBits, stopBits: $stopBits, parity: $parity, flowControl: $flowControl, timeoutMode: $timeoutMode, timeout: $timeout")
    val port = SerialPort.getCommPort(comPort)

    port.setBaudRate(baudRate)
    port.setFlowControl(flowControl)
    port.setComPortParameters(baudRate, dataBits, stopBits, parity)
    port.setComPortTimeouts(timeoutMode, timeout, timeout)

    val isOpen = port.openPort()

    if (!isOpen) {
      error(s"Port $comPort could not opened. Use the following documentation for troubleshooting: https://github.com/Fazecast/jSerialComm/wiki/Troubleshooting")

      throw new Exception(s"Port $comPort could not opened. Use the following documentation for troubleshooting: https://github.com/Fazecast/jSerialComm/wiki/Troubleshooting")
    }

    (port.getInputStream, port.getOutputStream)

  }
}
