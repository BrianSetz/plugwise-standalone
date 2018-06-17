package de.uni_stuttgart.reactive

import java.io.{InputStream, OutputStream}

import akka.actor.{ActorSystem, Cancellable, Props}
import akka.stream.IOResult
import akka.stream.actor.ActorPublisherMessage.Request
import akka.stream.actor.ActorSubscriberMessage.OnNext
import akka.stream.actor.{ActorPublisher, ActorSubscriber, RequestStrategy, WatermarkRequestStrategy}
import akka.stream.scaladsl.{Flow, Framing, Sink, Source, StreamConverters}
import akka.util.ByteString
import grizzled.slf4j.Logging
import org.reactivestreams.Subscriber

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{Future, Promise}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case object Tick
case class ReactiveMessage[T, U](value: T, promise: Option[Promise[U]] = None) {
  def map[A](f: T => A): ReactiveMessage[A, U] = {
    ReactiveMessage(f(value), promise)
  }

  def mapPromise[A](f: A => U): ReactiveMessage[T, A] = {
    val newPromise = Promise[A]
    promise.map(_.completeWith(newPromise.future.map(f)))
    ReactiveMessage(value, Some(newPromise))
  }

  def completeWith(result: U): ReactiveMessage[T, U] = {
    promise.map(_.success(result))
    this
  }
}

trait ReactiveApi {
  def byteStringDelimiter(delimiter: String, maxFrameLength: Int, allowTruncation: Boolean = true): Flow[ByteString, ByteString, _]
  def byteStringLogging  (prefix: String)                                                         : Flow[ByteString, ByteString, _]

  def streamSource(in: InputStream)                              : Source[ByteString, Future[IOResult]]
  def streamSink  (out: OutputStream, autoFlush: Boolean = true) :   Sink[ByteString, Future[IOResult]]

  def pushSource[T]()(implicit system: ActorSystem): (Source[T, _], T => _)
  def pullSource[T](f: => T, initialDelay: FiniteDuration = 1.second, interval: FiniteDuration = 1.second): Source[T, Cancellable]

  def subscriber[T](f: T => Unit)(implicit system: ActorSystem): Subscriber[T]
  def sink      [T](f: T => Unit)(implicit system: ActorSystem): Sink[T, _] = Sink.fromSubscriber(subscriber(f))
}

object Reactive extends ReactiveApi with Logging {
  override def byteStringDelimiter(delimeter: String, maxFrameLength: Int, allowTruncation: Boolean = true): Flow[ByteString, ByteString, _] =
    Framing.delimiter(ByteString(delimeter), maxFrameLength, allowTruncation)

  override def byteStringLogging(prefix: String): Flow[ByteString, ByteString, _] =
    Flow[ByteString].map(value => {
      if (value.nonEmpty)
        trace(s"$prefix: $value")

      value
    })

  override def streamSink(out: OutputStream, autoFlush: Boolean = true): Sink[ByteString, Future[IOResult]] =
    StreamConverters.fromOutputStream(() => out, autoFlush)

  override def streamSource(in: InputStream): Source[ByteString, Future[IOResult]] =
    StreamConverters.fromInputStream(() => in)

  override def pushSource[T]()(implicit system: ActorSystem): (Source[T, _], T => _) = {
    val actorRef     = system.actorOf(Props[ActorPublisherImpl[T]])
    val f: T => Unit = (t: T) => actorRef ! t
    val source       = Source.fromPublisher(ActorPublisher(actorRef))
    (source, f)
  }

  override def pullSource[T](f: => T, initialDelay: FiniteDuration, interval: FiniteDuration): Source[T, Cancellable] =
    Source.tick(initialDelay, interval, Tick).map(_ => f)

  override def subscriber[T](f: T => Unit)(implicit system: ActorSystem): Subscriber[T] = ActorSubscriber[T](system.actorOf(Props(new ActorSubscriberImpl(f))))
}

private[reactive] class ActorPublisherImpl[T] extends ActorPublisher[T] with Logging {
  val queue = new mutable.Queue[T]
  // TODO: limit the size of the internal queue; buffering should happen on a separate FLow buffer element - should probably even keep single element (?)

  @tailrec private def resolveBackPressure(): Unit = {
    if ((totalDemand > 0) && queue.nonEmpty) {
      onNext(queue.dequeue())
      resolveBackPressure()
    }
  }

  override def receive: Receive = {
    case Request(n)  =>
      trace(s"RS-Pub Request($n)")
      resolveBackPressure()

    case x => // TODO: ensure it is of type T (classTag, manifest, etc)!
      try {
        trace(s"RS-Pub Receive: $x")
        queue.enqueue(x.asInstanceOf[T])
        resolveBackPressure()
      } catch {
        case t: Throwable => error(s"Exception occured during RS-Pub: $x, exception: $t", t)
      }
  }
}

private[reactive] class ActorSubscriberImpl[T](f: T => Unit) extends ActorSubscriber with Logging {
  override protected def requestStrategy: RequestStrategy = WatermarkRequestStrategy(10) // TODO: set some reasonable configurable value

  override def receive: Receive = {
    case OnNext(m) => f(m.asInstanceOf[T])  // TODO: classtag/manifest for type safety

    case x => warn(s"Unexpected message in actor receive: $x")
  }
}