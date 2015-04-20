package core.models

import akka.actor.Address
import akka.cluster.Member

/**
 * Created by Ömer Faruk Gül on 12/03/15.
 */

case class BeginNodeJoin(node: Node)
case class BeginTransfer(partitionMaps: PartitionMaps)
case class TempPartitionMaps(partitionMaps: PartitionMaps)
//case class FinishJoin(node: Node)

//case class

case class Node(address:Address) {
	override def hashCode = address.##
	override def equals(other: Any) = other match {
		case m: Node ⇒ address == m.address
		case _         ⇒ false
	}
}