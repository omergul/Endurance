package core.actors

import akka.actor.{ActorLogging, Actor}
import akka.util.ByteString
import core.models._
import spray.http.{HttpEntity, HttpData, HttpResponse, StatusCodes}


import core.models.Data._
//import scala.collection.Map
import scala.collection.immutable.ListMap
import scala.collection.parallel.immutable

import core.models._
import core.utils.PartitionManager
import spray.http.HttpData
import spray.json._
import core.models.JsonPartitionResultProtocol._

/**
 * Created by Ömer Faruk Gül on 06/03/15.
 *
 * Partition Actor is responsible for insertion and retrievel of partition data.
 */
class PartitionActor(val partition:Partition) extends Actor with ActorLogging {

	//var partitions: Map[Int, Map[String, ByteString]] = Map()

	// todo: check if it is more appropriate to use ListMap
	//var store = Map.empty[String, ByteString]
	var store = ListMap.empty[String, (ByteString, Long)]
	//val store2 = scala.collection.mutable.HashMap.empty[String, ByteString]

	override def preStart() = {
		//log.info("Partition started with ID: "+partitionId)
		//log.info("Partition path: "+self.path)
	}

	def receive = {
		case InternalData(data, timestamp) =>
			store += (data.key -> (data.byteString, timestamp))
			sender() ! DataCreated
		case request:DataRequest =>
			//val bytesOption: Option[ByteString]  = store get request.key
			val tupleOption: Option[(ByteString, Long)]  = store get request.key
			tupleOption match {
				case Some((bytes, _)) =>
					sender() ! Data(request.key, bytes)
				case _ =>
					sender() ! DataNotFound
			}
		case request:PartitionDataRequest =>
			val list: List[Map[String, (Array[Byte], Long)]] = store.slice(request.offset, request.offset + request.limit).toList.map{case (k, v) => Map(k -> (v._1.toArray, v._2))}
			val result = JsonPartitionResult(request.partition.id, request.offset, request.limit, store.size, list)

			sender() ! result

		case msg =>
			log.info("Received something: "+msg)
	}

	override def postStop() = {
		log.info("Partition stopped with ID: "+partition.id)
	}
}
