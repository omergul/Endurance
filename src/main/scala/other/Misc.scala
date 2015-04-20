package other

import akka.actor.AddressFromURIString
import akka.util.ByteString
import core.models._
import core.utils.PartitionManager
import spray.http.HttpData
import spray.json._
import core.models.JsonPartitionResultProtocol._

import scala.collection.immutable.ListMap

/**
 * Created by Ömer Faruk Gül on 02/04/15.
 *
 * Description.
 */
object Misc {

	def listMapTest(offset:Int, limit:Int): Unit = {
		/*var list = ListMap.empty[String, ()]

		val count = 10
		(0 until count) foreach { i =>
			val key = "k"+i
			val bytes:ByteString = HttpData("hey: "+key).toByteString
			list += (key -> bytes)
		}

		val result1: List[Map[String, (Array[Byte], Long)]] = list.slice(offset, offset + limit).toList.map{case (k, v) => Map(k -> (v._1.toArray,))}
		//val resultList = result1.toList
		println("result1: "+result1)

		//val result2: List[(String, Array[Byte])] = result1.toList.map()

		val jsonResult = JsonPartitionResult(1, offset, limit, list.size, result1).toJson

		//val json = result2.toJson

		println("json: "+jsonResult.prettyPrint)

		val result3 = jsonResult.convertTo[JsonPartitionResult]
		var listMap2 = ListMap.empty[String, ByteString]

		println("The list: "+result3)
		result3.list.foreach{ map => listMap2 += (map.head._1 -> ByteString(map.head._2))}
		println("listMap:"+listMap2)
*/
	}

	def partitionTest2(): Unit = {
		val manager:PartitionManager = new PartitionManager(partitionCount = 31, backupCount = 4)
		val node1 = Node(AddressFromURIString("akka.tcp://1"))
		val node2 = Node(AddressFromURIString("akka.tcp://2"))
		val node3 = Node(AddressFromURIString("akka.tcp://3"))
		val node4 = Node(AddressFromURIString("akka.tcp://4"))
		val node5 = Node(AddressFromURIString("akka.tcp://5"))
		val node6 = Node(AddressFromURIString("akka.tcp://6"))

		manager.addNode(node1)
		manager.addNode(node2)
		manager.addNode(node3)
		manager.print()
		manager.addNode(node4)
		manager.print()
		manager.addNode(node5)
		manager.print()
		manager.addNode(node6)
		manager.print()

		val l1 = List(1,2,4,5)
		val l2 = List(2,3,4,6)

		val l3 = l1 diff l2
		val l4 = l2 diff l1
		println("l3: "+l3)
		println("l4: "+l4)
	}

	def partitionTest1(): Unit = {
		val manager:PartitionManager = new PartitionManager(partitionCount = 229, backupCount = 3)
		//val manager:PartitionManager = new PartitionManager(partitionCount = 8, backupCount = 3)
		val node1 = Node(AddressFromURIString("akka.tcp://1"))
		val node2 = Node(AddressFromURIString("akka.tcp://2"))
		val node3 = Node(AddressFromURIString("akka.tcp://3"))
		val node4 = Node(AddressFromURIString("akka.tcp://4"))
		val node5 = Node(AddressFromURIString("akka.tcp://5"))
		val node6 = Node(AddressFromURIString("akka.tcp://6"))

		manager.addNode(node1)
		manager.addNode(node2)
		manager.addNode(node3)
		manager.print()

		manager.addNode(node4)
		manager.print()

		/*
		manager.addNode(node5)
		manager.print()

		manager.removeNode(node1)
		manager.removeNode(node2)
		manager.print()*/
	}

	def partitionTest3(): Unit = {
		//val manager:PartitionManager = new PartitionManager(partitionCount = 229, backupCount = 3)
		val manager:PartitionManager = new PartitionManager(partitionCount = 6, backupCount = 2)
		val node1 = Node(AddressFromURIString("akka.tcp://a"))
		val node2 = Node(AddressFromURIString("akka.tcp://b"))
		val node3 = Node(AddressFromURIString("akka.tcp://c"))
		val node4 = Node(AddressFromURIString("akka.tcp://d"))
		val node5 = Node(AddressFromURIString("akka.tcp://e"))
		val node6 = Node(AddressFromURIString("akka.tcp://f"))
		val node7 = Node(AddressFromURIString("akka.tcp://g"))

		manager.addNode(node1)
		manager.print()

		manager.addNode(node2)
		manager.print()

		manager.addNode(node3)
		manager.print()

		manager.addNode(node4)
		manager.print()

		manager.addNode(node5)
		manager.print()

		manager.addNode(node6)
		manager.addNode(node7)
		manager.print()

		/*
		manager.addNode(node5)
		manager.print()

		manager.removeNode(node1)
		manager.removeNode(node2)
		manager.print()*/
	}
}
