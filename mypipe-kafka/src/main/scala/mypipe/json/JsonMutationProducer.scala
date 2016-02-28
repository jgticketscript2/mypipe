package mypipe.json

import com.typesafe.config.Config
import mypipe.api.event._
import mypipe.api.producer.Producer
import mypipe.kafka.{ KafkaUtil, KafkaProducer }
import org.slf4j.LoggerFactory

import scala.util.parsing.json.JSONObject

/** Created by mark.bittmann on 2/26/16.
 */
class JsonMutationProducer(config: Config) extends Producer(config) {
  type OutputType = Array[Byte]

  protected val delimtedBy = "\u0001"
  protected val terminatedBy = "\u0002"

  protected val metadataBrokers = config.getString("metadata-brokers")
  protected val producer = new KafkaProducer[OutputType](metadataBrokers)

  protected val logger = LoggerFactory.getLogger(getClass)
  protected def getKafkaTopic(mutation: Mutation): String = KafkaUtil.genericTopic(mutation)
  override def handleAlter(event: AlterEvent): Boolean = true // no special support for alters needed, schema embedded in json

  override def flush(): Boolean = {
    try {
      producer.flush
      true
    } catch {
      case e: Exception ⇒
        logger.error(s"Could not flush producer queue: ${e.getMessage} -> ${e.getStackTraceString}")
        false
    }
  }

  override def queue(mutation: Mutation): Boolean = {
    val database = mutation.database
    val table = mutation.table
    val txid = mutation.txid
    val keys = table.columns.map(_.name)
    val types = table.columns.map(_.colType)
    val primaryKeys = table.columns.map(_.isPrimaryKey)

    mutation match {
      case i: InsertMutation ⇒
        i.rows.foreach({ row ⇒
          val values = row.columns.values.map(_.value)
          val output = (keys, values, types).zipped.toList.map(tuple ⇒
            tuple._1 + delimtedBy + tuple._2.toString + delimtedBy + tuple._3).mkString(terminatedBy)

          //          println(output.split(terminatedBy).map(_.split(delimtedBy).mkString(":")).mkString("\t"))

          var combined = Map(
            "type" -> "insert",
            "db" -> database,
            "tbl" -> table.name,
            "tx_index" -> mutation.tx_idx,
            "data" -> output)

          if (txid != null) {
            combined = combined ++ Map("txid" -> txid.toString(), "ts" -> txid.timestamp())
          }
          producer.queue(getKafkaTopic(mutation), JSONObject(combined).toString().getBytes())

        })

      case u: UpdateMutation ⇒
        u.rows.foreach({ row ⇒
          val values = row._2.columns.values.map(_.value)
          val output = (keys, values, types).zipped.toList.map(tuple ⇒
            tuple._1 + delimtedBy + tuple._2.toString + delimtedBy + tuple._3).mkString(terminatedBy)
          var combined = Map(
            "type" -> "update",
            "db" -> database,
            "tbl" -> table.name,
            "txid" -> txid.toString(),
            "ts" -> txid.timestamp(),
            "tx_index" -> mutation.tx_idx,
            "data" -> output)

          producer.queue(getKafkaTopic(mutation), JSONObject(combined).toString().getBytes())
        })
      case d: DeleteMutation ⇒
        d.rows.foreach({ row ⇒
          val values = row.columns.values.map(_.value)
          val output = (keys, values, types).zipped.toList.map(tuple ⇒
            tuple._1 + delimtedBy + tuple._2.toString + delimtedBy + tuple._3).mkString(terminatedBy)
          var combined = Map(
            "type" -> "delete",
            "db" -> database,
            "tbl" -> table.name,
            "txid" -> txid.toString(),
            "ts" -> txid.timestamp(),
            "tx_index" -> mutation.tx_idx,
            "data" -> output)

          producer.queue(getKafkaTopic(mutation), JSONObject(combined).toString().getBytes())
        })
      case _ ⇒ logger.info(s"Ignored mutation: $mutation")
    }
    true
  }

  override def queueList(inputList: List[Mutation]): Boolean = {
    inputList.dropWhile(queue).isEmpty
  }

}
