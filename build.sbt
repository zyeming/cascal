name := "cascal"

version := "1.3-SNAPSHOT"

organization := "com.shorrockin"

scalaVersion := "2.9.0-1"

resolvers ++= Seq("Shorrockin Repository" at "http://maven.shorrockin.com/")

libraryDependencies ++= Seq(
	"org.apache.thrift" % "libthrift" % "0.6.1",
	"com.eaio.uuid" % "uuid" % "3.1",
	"org.apache.cassandra" % "cassandra-all" %  "0.8.5",
	"org.slf4j" % "slf4j-api" % "1.6.1",
	"org.slf4j" % "slf4j-log4j12" % "1.6.1",
	"log4j" % "log4j" % "1.2.14",
	"commons-pool" % "commons-pool" % "1.5.4",
	"junit" % "junit" % "4.6" % "test",
	"commons-lang" % "commons-lang" % "2.4" % "test",
	"commons-codec" % "commons-codec" % "1.2" % "test",
	"commons-collections" % "commons-collections" % "3.2.1" % "test",
	"com.google.clhm" % "clhm-production" % "1.0" % "test",
	"com.google.collections" % "google-collections" % "1.0" % "test",
	"flexjson" % "flexjson" % "1.7" % "test",
	"high-scale-lib" % "high-scale-lib" % "1.0" % "test"
	)
