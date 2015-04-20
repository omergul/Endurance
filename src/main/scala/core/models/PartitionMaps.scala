package core.models

import core.utils.Prefs

/**
 * Created by Ömer Faruk Gül on 15/03/15.
 *
 * Partition Maps holds the node list(in the cluster) and partition to node map.
 */

object PartitionMaps {
	def empty() = {
		PartitionMaps(List.empty[Node], Map.empty[Partition, List[Node]])
	}

	lazy val partitions:List[Partition] = (0 until Prefs.partitionCount).toList.map(i => Partition(i))
}

/*case class PartitionMaps(partitionToNodeMap: Map[Partition, List[Node]],
                         nodeToPartitionMap: Map[Node, List[Partition]]) {

}*/

case class PartitionMaps(nodes: List[Node], partitionToNodeMap: Map[Partition, List[Node]]) {
	def nodeToPartitionMap : Map[Node, List[Partition]] = {
		val partitions =  PartitionMaps.partitions
		nodes.map(node => node -> partitions.filter(partition => partitionToNodeMap(partition).contains(node))).toMap
	}
}
