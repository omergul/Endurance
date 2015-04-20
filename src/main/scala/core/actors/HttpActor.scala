package core.actors

import akka.actor.Status.Success
import akka.actor.{ActorLogging, Actor}
import akka.cluster.{Member, Cluster}
import akka.cluster.ClusterEvent.{UnreachableMember, LeaderChanged, MemberUp}
import core.models._
import core.utils.Prefs
import scala.concurrent.duration._
import akka.pattern.ask
import akka.util.{ByteString, Timeout}
import akka.actor._
import spray.can.Http
import spray.http._
import HttpMethods._
import scala.util.{Failure, Success}


class HttpActor(val dataRouter:ActorRef) extends Actor with ActorLogging {

	implicit val timeout: Timeout = 1.second

	val cluster = Cluster(context.system)

	var connectionOpenCount: Int = 0
	var connectionCloseCount: Int = 0

	//val clusterListener = context.actorOf(Props[ClusterListenerActor])
	//var objectBytes:Bytes = Bytes()

	override def preStart() = {
		log.info("SprayHttpActor started: "+self.path)
	}

	override def postStop() = {
		log.info("SprayHttpActor stopped.")
	}

	def receive = {

		case _: Http.Connected =>
			connectionOpenCount += 1
			sender() ! Http.Register(self)

		case request @ HttpRequest(GET, Uri.Path("/test"), _, _, _) =>

			val result = "Hi: "+request.uri.query.getOrElse("key",0.toString)
			sender() ! HttpResponse(status = StatusCodes.BadRequest, entity = HttpEntity(result))

		case request @ HttpRequest(GET, Uri.Path("/partition"), _, _, _) =>

			val partitionId = request.uri.query.getOrElse("partition","0").toInt
			val limit = request.uri.query.getOrElse("limit", "10").toInt
			val offset = request.uri.query.getOrElse("offset", "0").toInt

			dataRouter forward PartitionDataRequest(Partition(partitionId), offset, limit, DataSource.HTTP)

		case request @ HttpRequest(POST, Uri.Path("/object"), _, _, _) =>
			//log.info("Http headers: "+request.headers)
			val keyHeader:Option[HttpHeader] = request.headers find(header => header.name == "key")
			keyHeader match {
				case Some(header) =>
					//log.info("Saving object for key: "+header.value)
					val key = header.value
					val bytes: ByteString  = request.entity.data.toByteString

					dataRouter forward DataInsert(Data(key, bytes), DataSource.HTTP)

					/*
					val future = partitionManager ? Data(key, bytes)
					val currentSender = sender()
					future.onComplete{
						case util.Success(value) =>
							log.info("Asked return value: "+value)
							currentSender ! HttpResponse(status = StatusCodes.Created)
						case util.Failure(e) => e.printStackTrace()
					}
					*/
				case None => sender() ! HttpResponse(status = StatusCodes.BadRequest)
				case _ => sender() ! HttpResponse(status = StatusCodes.BadRequest)
			}

		/*case request @ HttpRequest(POST, Uri.Path("/transfer"), _, _, _) =>
			//log.info("Http headers: "+request.headers)
					//log.info("Saving object for key: "+header.value)
					val key = header.value
					val bytes: ByteString  = request.entity.data.toByteString

					dataRouter forward Data(key, bytes, DataSource.HTTP)*/

		case request @ HttpRequest(GET, Uri.Path("/object"), _, _, _) =>
			val keyHeader:Option[HttpHeader] = request.headers find(header => header.name == "key")
			keyHeader match {
				case Some(header) =>
					//log.info("Sending object for key: "+header.value)
					val key = header.value
					dataRouter forward DataRequest(key, DataSource.HTTP)

				case None => sender() ! HttpResponse(status = StatusCodes.BadRequest)
				case _ => sender() ! HttpResponse(status = StatusCodes.BadRequest)
			}

		case request @ HttpRequest(POST, Uri.Path("/echo"), _, _, _) =>
			val data: HttpData = request.entity.data
			val header = request.headers

			log.info("Http headers: "+request.headers)
			log.info("Echo request received.")

			sender() ! HttpResponse(entity = HttpEntity(data))

		case HttpRequest(GET, Uri.Path("/"), _, _, _) =>
			sender() ! HttpResponse(entity = HttpEntity("Hi Babe!"))

		case HttpRequest(GET, Uri.Path("/ping"), _, _, _) =>
			sender() ! HttpResponse(entity = HttpEntity("pong"))

		case _: Http.ConnectionClosed =>
			connectionCloseCount += 1
			log.info("Connection closed: "+connectionCloseCount)

		case Timedout(HttpRequest(method, uri, _, _, _)) =>
			sender ! HttpResponse(
				status = 500,
				entity = "The " + method + " request to '" + uri + "' has timed out..."
			)

		case msg =>
			log.info("Unknown message: "+msg)
	}
}
