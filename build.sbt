name := "streamsWorkshop"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
//  "org.apache.kafka" % "kafka-streams" % "2.5.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.5.0",
  "org.apache.kafka" % "kafka-streams-test-utils" % "2.5.0"
)

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0" % "test"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.slf4j" % "slf4j-simple" % "1.7.5")