name := "pubsubhub"

version := "1.0.0"

scalaVersion := "2.10.2"

libraryDependencies ++= Seq(
  "org.attribyte" % "attribyte-shared-base" % "1.0",
  "org.attribyte" % "acp" % "0.6.0",
  "org.attribyte" % "attribyte-http" % "0.5.1",
  "org.attribyte" % "essem-reporter" % "1.0.2",
  "org.attribyte" % "metrics-reporting" % "1.0.0",
  "commons-codec" % "commons-codec" % "1.4",
  "commons-httpclient" % "commons-httpclient" % "3.1",
  "commons-logging" % "commons-logging" % "1.1.3",
  "org.apache.commons" % "commons-pool2" % "2.2",
  "com.google.guava" % "guava" % "[15.0,)",
  "com.google.protobuf" % "protobuf-java" % "2.4.1",
  "org.eclipse.jetty.aggregate" % "jetty-all" % "9.2.7.v20150116",
  "log4j" % "log4j" % "1.2.17",
  "com.codahale.metrics" % "metrics-core" % "3.0.2",
  "com.codahale.metrics" % "metrics-healthchecks" % "3.0.2",
  "com.codahale.metrics" % "metrics-servlets" % "3.0.2",
  "com.codahale.metrics" % "metrics-jvm" % "3.0.2",
  "mysql" % "mysql-connector-java" % "5.1.29",
  "com.typesafe" % "config" % "1.0.2",
  "org.antlr" % "ST4" % "4.0.7",
  "com.h2database" % "h2" % "1.4.185",
  "org.slf4j" % "slf4j-log4j12" % "1.7.5",
  "junit" % "junit" % "4.11"
)

XitrumPackage.copy()