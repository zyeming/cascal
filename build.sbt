name := "cascal"

version := "1.3-SNAPSHOT"

organization := "com.shorrockin"

scalaVersion := "2.9.1"

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
    "org.apache.cassandra" % "cassandra-all" %  "0.8.7",
    "org.apache.thrift" % "libthrift" % "0.6.1",
	"com.eaio.uuid" % "uuid" % "3.2",
	"org.slf4j" % "slf4j-api" % "1.6.1",
	"org.slf4j" % "slf4j-log4j12" % "1.6.1",
	"log4j" % "log4j" % "1.2.14",
	"commons-pool" % "commons-pool" % "1.5.4",
	"junit" % "junit" % "4.6" % "test",
	"commons-lang" % "commons-lang" % "2.4" % "test",
	"commons-codec" % "commons-codec" % "1.2" % "test",
	"commons-collections" % "commons-collections" % "3.2.1" % "test",
	"com.google.clhm" % "clhm-production" % "1.0" % "test",
	"flexjson" % "flexjson" % "1.7" % "test",
	"com.github.stephenc.high-scale-lib" % "high-scale-lib" % "1.1.2" % "test",
	"com.googlecode.concurrentlinkedhashmap" % "concurrentlinkedhashmap-lru" % "1.1",
	"com.google.guava" % "guava" % "r08" % "test",
	"com.github.stephenc" % "jamm" % "0.2.2" % "test",
	"org.apache.cassandra.deps" % "avro" % "1.4.0-cassandra-1" % "test",
	"org.antlr" % "antlr" % "3.2" % "test"
  )
	
libraryDependencies += "com.novocode" % "junit-interface" % "0.7" % "test->default"

