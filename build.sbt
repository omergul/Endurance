name := "Endurance"

version := "1.0"

scalaVersion := "2.11.5"

libraryDependencies ++= Seq(
	"com.typesafe.akka" % "akka-actor_2.11" % "2.4-SNAPSHOT",
	"com.typesafe.akka" % "akka-slf4j_2.11" % "2.4-SNAPSHOT",
	"com.typesafe.akka" % "akka-cluster_2.11" % "2.4-SNAPSHOT",
	"io.spray" %% "spray-can" % "1.3.2",
	"io.spray" %% "spray-json" % "1.3.1"
)

libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.5"

resolvers += "Akka Snapshot Repository" at "http://repo.akka.io/snapshots/"

resolvers += "spray repo" at "http://repo.spray.io"