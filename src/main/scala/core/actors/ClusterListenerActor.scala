package core.actors

import akka.actor.{RootActorPath, ActorPath, Address, Actor}
import akka.cluster.{Member, MemberStatus, Cluster}
import akka.cluster.ClusterEvent._
import akka.util.{Timeout, ByteString}
import core.models._
import core.utils.{Prefs, PartitionManager}
import akka.pattern.ask
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Created by Ömer Faruk Gül on 08/03/15.
 */
class ClusterListenerActor extends Actor {

	implicit val timeout: Timeout = 2.seconds

	val cluster = Cluster(context.system)
	//var members = IndexedSeq.empty[Member]
	var members = List.empty[Member]
	var leader:Option[Address] = None

	var nodeJoining = false

	val partitionManager = new PartitionManager(Prefs.partitionCount, Prefs.backupCount)

	override def preStart(): Unit = {
		println("Listener actor PATH: "+ self.path.toString)
		println("Listener actor CLUSTER: "+ cluster.selfAddress)
		//cluster.subscribe(self, classOf[ClusterDomainEvent])
		cluster.subscribe(self, classOf[MemberEvent], classOf[LeaderChanged], classOf[UnreachableMember], classOf[ReachableMember])
	}

	override def postStop() = {
		cluster.unsubscribe(self)
	}

	def receive = {

		case state:CurrentClusterState =>
			println("current state members: "+state.members)
			println("current state leader: "+state.leader)
			updateLeader(state.leader)
			state.members.filter(_.status == MemberStatus.up) foreach register
		case LeaderChanged(leaderOption) =>
			updateLeader(leaderOption)
		case MemberUp(member) =>
			register(member)
		case UnreachableMember(member) =>
			println("Unreachable member: "+member)
			unregister(member)
		case ReachableMember(member) =>
			println("Reachable member: "+member)
			register(member)
		case MemberRemoved(member, prevStatus) =>
			println("Member removed: "+member)
			unregister(member)


		case BeginNodeJoin(node) =>
			nodeJoining = true
			partitionManager.addNode(node)

			import concurrent.ExecutionContext.Implicits.global

			// nodes to be updated
			val nodes: List[Node] = partitionManager.selectedPartitions.map{partition => partitionManager.tempPartitionToNodeMap(partition).head}.distinct
			println("Nodes to be updated with temp: "+nodes)

			val futures: List[Future[Any]] = nodes.map{node =>
				val path: ActorPath =  RootActorPath(node.address) / "user" / "dataRouter"
				context.actorSelection(path) ? TempPartitionMaps(partitionManager.tempPartitionMaps)}

			val list: Future[List[Any]] = Future.sequence(futures)
			list onComplete {
				case Success(results) => println("Temp partitions delivered successfully.")
				case Failure(ex) => println("Exception has occurred: "+ex)
			}

			//sender() ! BeginTransfer(partitionManager.tempPartitionMaps)

		case BeginTransfer(partitionMaps) =>
			// we have

		/*case msg =>
			//println("Listener event: "+msg)*/
	}

	def updateLeader(leader:Option[Address]) = {
		println("Leader updated: "+leader)
		this.leader = leader
		leader match {
			case Some(memberAddress) =>
				// leader is us!
				if(memberAddress == cluster.selfAddress) {
					println("We are the leader!")
				}
			case None => // don't do anything
		}
	}

	def unregister(member: Member): Unit = {
		members = members diff List(member)
		println("Total members count after unregister: "+members.size)
		println("Members: "+members)

		partitionManager.removeNode(Node(member.address))

		//broadcastPartitionMap()
	}

	def register(member: Member): Unit = {
		members = members ++ List(member)
		println("Total members count after register: "+members.size)
		println("Members: "+members)

		//partitionManager.addNode(Node(member.address.hashCode, member.address))

		if(member.address == cluster.selfAddress) {
			println("IT IS US!!!!")
			// we have to ask a node for the partition data

			startClusterJoin()
		}

		//broadcastPartitionMap()

		/*val path: ActorPath =  RootActorPath(member.address) / "user" / "handler"
		println("New member path: "+path)
		println("New member address: "+RootActorPath(member.address).address)*/
	}

	def startClusterJoin(): Unit = {

		this.leader match {
			case Some(leaderAddress) =>
				if(leaderAddress == cluster.selfAddress) {
					println("We are the added member and leader, we don't join anyone!")
					partitionManager.addNode(Node(cluster.selfAddress))

					//context.actorSelection("../dataRouter") ! DataInsert(Data("test1", ByteString("aa")), DataSource.OTHER)
					context.actorSelection("../dataRouter") ! partitionManager.partitionMaps
				}
				else {
					val path: ActorPath =  RootActorPath(leaderAddress) / "user" / "clusterListener"
					context.actorSelection(path) ! BeginNodeJoin(Node(cluster.selfAddress))

					/*val result = Await.result(future, 3.seconds).asInstanceOf[PartitionMaps]
					partitionManager.updateExternally(result)
					println("The asked partition maps result is!")
					partitionManager.print()*/

					/*
					val future2 = httpActor ? partitionManager.partitionMaps
					val result2 = Await.result(future2, 2.seconds).asInstanceOf[String]
					println("http update result: "+result2)

					val future3 = transferActor ? partitionManager.partitionMaps
					val result3 = Await.result(future3, 2.seconds).asInstanceOf[String]
					println("transfer actor result: "+result3)*/
				}
			case None =>
				println("There is no leader defined!")
		}
	}

	def broadcastPartitionMap(): Unit = {
		/*val partitionMaps = PartitionMaps(partitionManager.partitionToNodeMap, partitionManager.nodeToPartitionMap)
		members foreach{member =>
			val path: ActorPath =  RootActorPath(member.address) / "user" / "http"
			context.actorSelection(path) ! partitionMaps
		}*/
	}
}
