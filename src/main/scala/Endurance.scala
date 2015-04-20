import java.net.BindException

import akka.actor._
import akka.cluster.{Member, Cluster}
import akka.io.IO
import akka.io.Tcp.CommandFailed
import akka.util.{ByteString, Timeout}
import core.actions.InsertDummyData
import core.actors.NodeActor
import core.models._
import core.utils.PartitionManager
import other.Misc
import spray.can.Http.Bind
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.pattern.ask
import com.typesafe.config.{Config, ConfigValueFactory, ConfigFactory}
import org.jboss.netty.channel.ChannelException
import spray.can.Http
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Try, Failure, Success}

/**
 * Created by Ömer Faruk Gül on 01/03/15.
 *
 * Endurance main app
 */

object Endurance extends App {
	println("Endurance starting.")

	val endurance: Endurance = new Endurance(isSeed = true)
	endurance.start()
	endurance.initCommandLine()

	//Misc.partitionTest3()

	//Misc.listMapTest(0,11)
}


class Endurance(val isSeed:Boolean) {

	val config = ConfigFactory.load()
	//val level = config.getString("akka.loglevel")
	private var akkaPort = 2501

	var portTryCount = 0
	var maxPortIncrement = 100

	var system: ActorSystem  = _

	var nodeActor:ActorRef = _

	def start() = {
		val newConfig: Config = config.withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(akkaPort))
		val systemName = "Endurance"

		system = initSystem(systemName, newConfig)

		val cluster = Cluster(system)
		if(isSeed) {
			cluster.joinSeedNodes(List(cluster.selfAddress))
		}
		else {
			cluster.join(AddressFromURIString("akka.tcp://" + systemName + "@127.0.0.1:2501"))
		}

		nodeActor = system.actorOf(Props[NodeActor], name = "node")
	}

	def initCommandLine(): Unit = {
		for (ln <- io.Source.stdin.getLines()) {
			executeCommand(line = ln)
		}
	}

	private def executeCommand(line:String): Unit = {
		line.split("\\s+").toList match {
			case "InsertDummyData" :: keyPrefix :: count :: Nil =>
				nodeActor ! InsertDummyData(keyPrefix, count.toInt)
				println(s"Inserted dummy data: $keyPrefix, $count")
			case "insert" :: key :: value :: Nil =>
				nodeActor ! DataInsert(Data(key, ByteString(value)), DataSource.OTHER)
				println(s"Inserted data: $key: $value")
			case "get" :: key :: Nil =>
				implicit val timeout = Timeout(5 seconds)
				val future = nodeActor ? DataRequest(key, DataSource.OTHER)
				val data = Await.result(future, timeout.duration).asInstanceOf[Data]
				val str = data.byteString.utf8String
				println(s"Result data: ${data.key}: $str")
			case _ => println("Command not found.")
		}
	}

	private def initSystem(systemName:String, config:Config): ActorSystem = {
		val sys:ActorSystem = Try(ActorSystem(systemName, config)) match {
			case Success(s) => s
			case Failure(ex) => ex match {
				case e:ChannelException =>
					portTryCount += 1
					if(portTryCount > maxPortIncrement) {
						throw e
					}

					akkaPort += 1
					val newConfig: Config = config.withValue("akka.remote.netty.tcp.port", ConfigValueFactory.fromAnyRef(akkaPort))
					initSystem(systemName, newConfig)
				case e => throw e
			}
		}

		// return the sytem
		sys
	}
}
