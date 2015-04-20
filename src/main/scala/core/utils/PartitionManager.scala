package core.utils

import core.models.{Node, Partition, PartitionMaps}

/**
 * Created by Ömer Faruk Gül on 12/03/15.
 *
 * Handles partitioning and redistribution algorithm.
 *
 */
class PartitionManager(val partitionCount:Int, val backupCount:Int) {

	var nodes:List[Node] = List.empty[Node]

	var partitionToNodeMap:Map[Partition, List[Node]] = Map()
	var tempPartitionToNodeMap:Map[Partition, List[Node]] = Map()

	val partitions =  PartitionMaps.partitions

	var selectedPartitions:List[Partition] = List()

	partitionToNodeMap = partitions.map(partition => partition -> nodes).toMap

	def isEmpty: Boolean = {
		partitionToNodeMap.isEmpty
	}

	def partitionMaps:PartitionMaps = {
		PartitionMaps(nodes, partitionToNodeMap)
	}

	def tempPartitionMaps:PartitionMaps = {
		PartitionMaps(nodes, tempPartitionToNodeMap)
	}

	def addNode(node:Node): Unit = {

		nodes = nodes diff List(node)
		nodes = nodes ::: List(node)

		val r = scala.util.Random

		if(nodes.size <= backupCount) {
			tempPartitionToNodeMap = partitions.map(partition => partition -> nodes).toMap
			partitionToNodeMap = partitions.map(partition => partition -> r.shuffle(nodes)).toMap
		}
		else {
			val partitionPerNode = (partitionCount * backupCount) / nodes.size

			// select the new partitions that will be on the new node
			selectedPartitions = r.shuffle(partitions).take(partitionPerNode)

			// put the picked partitions to new node, and remove it from old nodes
			tempPartitionToNodeMap = partitionToNodeMap.map { case (partition, nodeList) =>
				if (selectedPartitions.contains(partition)) partition ->
					(nodeList ::: List(node))
				//(List(node) ::: r.shuffle(nodeList).take(backupCount - 1))
					//r.shuffle(List(node) ::: r.shuffle(nodeList).take(backupCount - 1))
				else partition -> nodeList
			}

			partitionToNodeMap = partitionToNodeMap.map { case (partition, nodeList) =>
				if (selectedPartitions.contains(partition)) partition ->
				r.shuffle(List(node) ::: r.shuffle(nodeList).take(backupCount - 1))
				//r.shuffle(List(node) ::: r.shuffle(nodeList).take(backupCount - 1))
				else partition -> nodeList
			}

		}

		/*
		partitionToNodeMap = partitionToNodeMap.map { case (partition, nodeList) =>
				if (selectedPartitions.contains(partition)) partition ->
					(nodeList.take(backupCount - 1) ::: List(node))
				//(List(node) ::: r.shuffle(nodeList).take(backupCount - 1))
					//r.shuffle(List(node) ::: r.shuffle(nodeList).take(backupCount - 1))
				else partition -> nodeList
			}
		 */
	}

	def removeNode(node:Node): Unit = {
		nodes = nodes diff List(node)

		val r = scala.util.Random

		// remove the node from partitions
		partitionToNodeMap = partitionToNodeMap.map { case (partition, nodeList) =>
			partition -> (nodeList diff List(node))
		}

		val replicationCount = if(backupCount <= nodes.size) backupCount else nodes.size
		println("replication count :"+replicationCount)

		partitionToNodeMap = partitionToNodeMap.map { case (partition, nodeList) =>
			partition ->  {
				if(nodeList.size < replicationCount) nodeList ::: r.shuffle(nodes diff nodeList).take(replicationCount - nodeList.size)
				else nodeList
			}
		}
	}

	def updateExternally(maps: PartitionMaps) = {
		this.partitionToNodeMap = maps.partitionToNodeMap
		this.nodes = maps.nodes
	}

	def print() = {

		println("------------- partition maps -------------")

		println("----- Temp: -----")
		tempPartitionToNodeMap.toSeq.sortBy(_._1.id).foreach{case(key, value) =>
			printf(key+":")
			//val str = value.sortBy(_.address.host).map(node => node.address.hostPort).mkString(",")
			val str = value.map(node => node.address.hostPort).mkString(",")
			println(str)
		}

		println("----- Permanent: -----")
		partitionToNodeMap.toSeq.sortBy(_._1.id).foreach{case(key, value) =>
				printf(key+":")
				//val str = value.sortBy(_.address.host).map(node => node.address.hostPort).mkString(",")
				val str = value.map(node => node.address.hostPort).mkString(",")
				println(str)
		}

		this.partitionMaps.nodeToPartitionMap.toSeq.sortBy(_._1.address.host).foreach{case(key, list) =>
				printf(key+":")
				//val str = value.map(partition => partition.id).mkString(",")
				//println(str)
				val str = list.sortBy(_.id).map(partition => partition.id).mkString(",")
				println(list.size+" : "+str)
		}
	}
}
