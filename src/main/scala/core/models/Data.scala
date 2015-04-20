package core.models

import akka.util.ByteString
import spray.json.DefaultJsonProtocol

/**
 * Created by Ömer Faruk Gül on 06/03/15.
 */

object DataSource extends Enumeration {
	type DataSource = Value
	val HTTP= Value("HTTP")
	val OTHER = Value("OTHER")
}

case class Data(key: String, byteString: ByteString)
case class InternalData(data:Data, timestamp: Long)
case class DataRequest(key: String, source: DataSource.DataSource)
case class DataInsert(data: Data, source: DataSource.DataSource)
case object DataCreated
case object DataNotFound

//case class PartitionData(partition: Partition, offset: Int, limit: Int, list:List[Data])
case class PartitionDataRequest(partition: Partition, offset: Int, limit: Int, source: DataSource.DataSource)

case class JsonPartitionResult(partitionId:Int, offset:Int, limit:Int, total:Int, list:List[Map[String, (Array[Byte], Long)]])
object JsonPartitionResultProtocol extends DefaultJsonProtocol {
	implicit val jsonDataResultFormat = jsonFormat5(JsonPartitionResult)
}

