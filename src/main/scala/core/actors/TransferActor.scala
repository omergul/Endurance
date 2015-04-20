package core.actors

import akka.actor.{ActorRef, ActorLogging, Actor}
import akka.cluster.Cluster
import core.models.{Node, BeginTransfer, PartitionMaps, Partition}

/**
 * Created by Ömer Faruk Gül on 16/03/15.
 *
 * Description.
 */
class TransferActor(val cluster:Cluster, val partitionActors:Map[Partition, ActorRef]) extends Actor with ActorLogging {

	var partitionMaps:PartitionMaps = PartitionMaps.empty()

	def receive = {

/*		case BeginTransfer(newPartitionMaps:PartitionMaps, currentPartitionMaps:PartitionMaps) =>
			println("Transfer actor will start transferring!")

			val currentNode:Node = Node(cluster.selfAddress)

			// we need to suspend the transferred partitions.
			val transferredPartitions: List[Partition] = newPartitionMaps.nodeToPartitionMap(currentNode) diff partitionMaps.nodeToPartitionMap(currentNode)
			
			sender() ! "ok"*/
		case msg =>
			println("Transfer actor received message!")
	}

}
