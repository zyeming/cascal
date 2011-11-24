name := "cascal"

version := "1.3-SNAPSHOT"

organization := "com.shorrockin"

scalaVersion := "2.9.1"

compileOrder := CompileOrder.JavaThenScala

libraryDependencies ++= Seq(
    "org.apache.cassandra" % "cassandra-all" %  "1.0.3",
    "org.apache.thrift" % "libthrift" % "0.6.1",
	"com.eaio.uuid" % "uuid" % "3.2",
	"org.slf4j" % "slf4j-api" % "1.6.1",
	"commons-pool" % "commons-pool" % "1.5.4",
	"junit" % "junit" % "4.6" % "test"
  )
	
libraryDependencies += "com.novocode" % "junit-interface" % "0.7" % "test->default"

