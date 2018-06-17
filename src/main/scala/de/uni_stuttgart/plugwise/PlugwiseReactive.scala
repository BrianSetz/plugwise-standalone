package de.uni_stuttgart.plugwise

import akka.stream.{FlowShape, OverflowStrategy, Supervision}
import akka.stream.scaladsl._
import akka.util.ByteString
import grizzled.slf4j.Logging

import scala.concurrent.duration.FiniteDuration

trait PlugwiseReactive extends PlugwiseUtils with Logging {
  case class ExpectedInvalidPacketException(msg: String) extends Exception(msg)
  case class InvalidPacketException(msg: String) extends Exception(msg)


  val supervisor: Supervision.Decider = {
    case ex: ExpectedInvalidPacketException =>
      // We expect this to happen regularly for messages such as:
      // # APSRequestNodeInfo: Source MAC: 000D6F00019E0E74# APSRequestNodeInfo: Destination MAC: 000D6F0001A44F98
      trace(s"Supervisor Expected Exception: ${ex.getMessage}")
      Supervision.Resume

    case t: Throwable =>
      error(s"Supervisor Exception: ${t.getMessage}", t)
      Supervision.Resume

    case x =>
      error(s"Supervisor UNKNOWN: $x")
      Supervision.Resume
  }

  def cleanPacket: Flow[ByteString, ByteString, _] = Flow[ByteString].map(bytes => {
    def dropBytes(byteString: ByteString): ByteString = {
      if (!byteString.startsWith(Seq(5, 5, 3, 3)) && byteString.nonEmpty) {
        dropBytes(byteString.drop(1))
      } else if (byteString.isEmpty) {
        val byteToString = bytes.map(_.toChar).mkString

        if(byteToString.startsWith("# APSRequestNodeInfo") || byteToString.startsWith("000D6F"))
          throw ExpectedInvalidPacketException(s"Expected invalid packet: $byteToString")
        else
          throw InvalidPacketException(s"Unexepcted invalid packet. No packet start for: $byteToString")
      } else {
        byteString
      }
    }

    dropBytes(bytes) ++ ByteString(13, 10)
  })

  def throttle[T](rate: FiniteDuration): Flow[T, T, _] = {
    Flow.fromGraph(GraphDSL.create() { implicit builder =>
      import GraphDSL.Implicits._

      val zip = builder.add(Zip[T, Unit.type]())
      Source.tick(rate, rate, Unit).buffer(1, OverflowStrategy.dropNew) ~> zip.in1

      FlowShape(zip.in0, zip.out)
    }).map(_._1)
  }


  def byteStringToResponse: Flow[ByteString, ResponsePacket, _] = Flow[ByteString].map(bytes => {
    val raw = bytes.map(_.toChar).mkString.stripLineEnd.substring(4)

    val responseCode = raw.substring(0,4)
    val sequenceNumber = raw.substring(4,8)

    ResponsePacket(responseCode, sequenceNumber, raw)
  })

  def makePacket: Flow[RequestPacket, ByteString, _] = Flow[RequestPacket].map(request => {
    ByteString((Packets.Start + request.rawPacket.toUpperCase + crc(request.rawPacket.toUpperCase) + Packets.End).getBytes)
  })

  def printOutput(prefix: String, f: Any => Unit): Flow[ByteString, ByteString, _] = Flow[ByteString].map(message => {
    f(s"$prefix ${message.map(_.toChar).mkString.replace("\r\n", "\\r\\n")} (${message.mkString(" ")})")

    message
  })
}
