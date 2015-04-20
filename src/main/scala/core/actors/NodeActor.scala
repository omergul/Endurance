package core.actors

import akka.actor._
import akka.cluster.ClusterEvent._
import akka.cluster.{MemberStatus, Cluster, Member}
import akka.io.IO
import akka.io.Tcp.{Bound, Connected, CommandFailed}
import akka.util.{ByteString, Timeout}
import core.actions.InsertDummyData
import core.models.Node
import core.utils.{Prefs, PartitionManager}
import spray.can.Http
import spray.can.Http.Bind
import spray.http.HttpData
import scala.concurrent.{Future, Await}
import scala.concurrent.duration._
import akka.pattern.ask
import core.models._
import scala.concurrent.ExecutionContext.Implicits.global
import akka.pattern.pipe

import scala.util.{Failure, Success}

/**
 * Created by Ömer Faruk Gül on 14/03/15.
 *
 * NodeActor is the root actor in a node, which initiates HttpActor and PartitionActors and other helper classes.
 */

/*case object AskPartitionMaps
case object AskStartingTransfer
case object AskEndingTransfer*/

class NodeActor extends Actor with ActorLogging {

	implicit val timeout: Timeout = 2.seconds

	val cluster = Cluster(context.system)

	val partitionActors: Map[Partition, ActorRef] = (0 until Prefs.partitionCount).map(i =>
		Partition(i) -> context.actorOf(Props(classOf[PartitionActor], Partition(i)), name = s"partition$i")
	).toMap

	val clusterListenerActor = context.actorOf(Props(classOf[ClusterListenerActor]), name = "clusterListener")
	val dataRouterActor = context.actorOf(Props(classOf[DataRouterActor], partitionActors), name = "dataRouter")
	val transferActor = context.actorOf(Props(classOf[TransferActor], cluster, partitionActors), name = "transfer")
	val httpActor = context.actorOf(Props(classOf[HttpActor],dataRouterActor), name = "http")

	var httpPort = 8080

	// bind to http
	bind(httpActor, "localhost", httpPort)

	def bind(handler: ActorRef, interface:String, port:Int): Unit = {
		implicit val timeout = Timeout(5.seconds)
		import context.system

		val future = IO(Http) ? Http.Bind(handler, interface, port)
		future.onComplete{
			case Success(Bound(address)) =>
				println("HTTP actor bounded to: "+address)
			case Success(CommandFailed(_:Bind)) =>
				// try again by increasing the port number
				bind(handler, interface, port + 1)
			case Success(CommandFailed(msg)) =>
				println("IO CommandFailed: "+msg)
			case Failure(ex) =>
				println("IO Future failure: "+ex)
			case msg =>
				println("IO some message: "+msg)
		}
	}

	override def preStart() = {
		log.info("Node started: "+self.path)
	}

	override def postStop() = {
		log.info("Node stopped.")
	}

	def receive = {
		case InsertDummyData(keyPrefix, count) =>
			println("Inserting dummy data.")
			(0 until count) foreach { i =>
				val key = keyPrefix+i
				val bytes:ByteString = HttpData("hey: "+key).toByteString
				dataRouterActor ! DataInsert(Data(key, bytes), DataSource.OTHER)
			}
		case dataInsert:DataInsert =>
			dataRouterActor ! dataInsert
		case dataRequest:DataRequest =>
			val future = dataRouterActor ? dataRequest
			future pipeTo sender()
		case DataCreated =>
			// nothing to do

		/*case BeginNodeJoin(node) =>
			partitionManager.addNode(node)
			val futures = partitionManager.nodes.map{node =>
				val path: ActorPath =  RootActorPath(node.address) / "user" / "transfer"
				context.actorSelection(path) ? BeginTransfer(partitionManager.partitionMaps)}*/

		case msg =>
			println("Node received something: "+msg)
	}
}
