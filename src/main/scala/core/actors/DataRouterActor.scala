package core.actors

import akka.actor.{Stash, ActorLogging, Actor, ActorRef}
import akka.cluster.Cluster
import akka.util.Timeout
import core.models._
import core.utils.Prefs
import spray.http._
import scala.concurrent.duration._
import akka.pattern.ask
import scala.util.Success
import spray.json._
import core.models.JsonPartitionResultProtocol._
import spray.http.HttpHeaders._
import spray.http.ContentTypes._

/**
 * Created by Ömer Faruk Gül on 18/03/15.
 *
 * DataRouter actor is responsible for routing data to other routers and insertion-retrieval of data
 */

class DataRouterActor(val partitionActors:Map[Partition, ActorRef]) extends Actor with ActorLogging with Stash{

	import context.dispatcher
	implicit val timeout: Timeout = 1.second

	var partitionMaps:PartitionMaps = PartitionMaps.empty()
	var oldPartitionMaps:PartitionMaps = PartitionMaps.empty()

	val cluster = Cluster(context.system)

	val currentNode = Node(cluster.selfAddress)

	override def preStart() = {
		println("DataMa started.")
	}

	override def postStop() = {
		println("DataRouter stopped.")
	}

	def receive = {
		// update partitions
		case maps:PartitionMaps =>
			println("DataRouterActor: updating partition maps for the first time")
			this.oldPartitionMaps = this.partitionMaps
			this.partitionMaps = maps
			sender() ! PartitionMapsUpdated
			context.become(active, discardOld = false)
			unstashAll()
		case _ =>
			println("DataRouterActor: stashing the request")
			stash()
	}

	def active:Receive = {

		// update partitions
		case maps:PartitionMaps =>
			println("DataRouterActor: updating partition maps")
			this.oldPartitionMaps = this.partitionMaps
			this.partitionMaps = maps
			sender() ! PartitionMapsUpdated

		/*
		 * Handles data insert
		 */
		case DataInsert(data, source) =>

			val partition = Partition(partitionId(data.key))

			val nodes: List[Node] = this.partitionMaps.partitionToNodeMap(partition)
			if(nodes.head == currentNode) {
				println("Yes, we are writing to this node.")

				val partitionActor: ActorRef = partitionActors(partition)

				val future = partitionActor ? InternalData(data, System.nanoTime())
				val currentSender = sender()
				future.onComplete{
					case Success(value) =>
						if(source == DataSource.HTTP)
							currentSender ! HttpResponse(status = StatusCodes.Created)
						else
							currentSender ! DataCreated
					case util.Failure(e) => e.printStackTrace()
				}
			}
			else {
				println("Write request needs to be forwarded.")
			}

			//val nodes:List[Node] = partitionMaps.partitionToNodeMap(partition)
			//println("Saving data, our address : "+cluster.selfAddress)

		/*
		 * Handles data request
		 */
		case request @ DataRequest(key, source) =>
			val partitionActor = partitionActors(Partition(partitionId(key)))
			val future = partitionActor ? request
			val currentSender = sender()
			future.onComplete {
				case Success(data:Data) =>

					//val d = Map.empty[String, String];
					//val d1 = HttpData(d.byt)
					if(source == DataSource.HTTP) {
						val httpData = HttpData(data.byteString)
						currentSender ! HttpResponse(status = StatusCodes.OK, entity = HttpEntity(httpData))
					}
					else
						currentSender ! data
				case Success(DataNotFound) =>
					if(source == DataSource.HTTP)
						currentSender ! HttpResponse(status = StatusCodes.BadRequest)
					else
						currentSender ! DataNotFound
				case _ =>
					currentSender ! HttpResponse(status = StatusCodes.BadRequest)
			}

		// partition request
		case request @ PartitionDataRequest(partition, _, _, source) =>
			val partitionActor = partitionActors(request.partition)
			val future = partitionActor ? request
			val currentSender = sender()
			future.onComplete {
				case Success(result:JsonPartitionResult) =>
					val httpData = HttpData(result.toJson.toString())
					if(source == DataSource.HTTP) {
						//headers = List(`Content-Type`(`application/json`))
						val response = HttpResponse(status = StatusCodes.OK, entity = HttpEntity(`application/json`, httpData))
						currentSender ! response
					}
					else
						currentSender ! result
				/*
				case Success(DataNotFound) =>
					if(source == DataSource.HTTP)
						currentSender ! HttpResponse(status = StatusCodes.BadRequest)
					else
						currentSender ! DataNotFound
				*/
				case _ =>
					currentSender ! HttpResponse(status = StatusCodes.BadRequest)
			}

		case msg => println("Data manager received a message: "+msg)
	}

	def partitionId(key:String):Int = {
		key.hashCode % Prefs.partitionCount
	}
}
