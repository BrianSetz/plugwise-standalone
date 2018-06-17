name := "plugwise-standalone"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "joda-time" % "joda-time" % "2.10",
  "org.clapper" %% "grizzled-slf4j" % "1.3.2",
  "com.typesafe.akka" %% "akka-stream" % "2.5.13",
  "com.fazecast" % "jSerialComm" % "2.0.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
