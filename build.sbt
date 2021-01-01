import sbt.Keys._

name := "kafka-avro"

version := "0.1"

scalaVersion := "2.13.4"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.7.0",
  "org.apache.zookeeper" % "zookeeper" % "3.6.2",
  "org.apache.avro" % "avro" % "1.10.1",
  "io.confluent" % "kafka-avro-serializer" % "6.0.1",
  "com.google.code.gson" % "gson" % "2.8.6"
)

Compile / packageAvro / publishArtifact := true

resolvers += "Confluent" at "https://packages.confluent.io/maven"
