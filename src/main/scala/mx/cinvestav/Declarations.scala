package mx.cinvestav

import cats.data.EitherT
import cats.effect.std.Queue
import cats.effect.{IO, Ref}
import io.chrisdavenport.mules.MemoryCache
import mx.cinvestav.commons.balancer
import mx.cinvestav.commons.balancer.v2.Balancer
import mx.cinvestav.commons.errors.NodeError
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.DefaultConfigV5
import mx.cinvestav.utils.v2.{PublisherV2, RabbitMQContext}
import mx.cinvestav.commons.compression
import mx.cinvestav.server.HttpServer.User
import org.http4s.{AuthedRequest, Request}
import org.typelevel.log4cats.Logger

import java.io.File
import java.util.UUID

object Declarations {
  def liftFF[A]: IO[A] => EitherT[IO, NodeError, A] =  commons.liftFF[A,NodeError]
//
  object CommandIds {
    final val ADD_NODE    = "ADD_NODE"
    final val REMOVE_NODE = "REMOVE_NODE"
    final val REPLICATE     = "REPLICATE"
    final val PULL     = "PULL"
  //
    final val PROPOSE     = "PROPOSE"
    final val PREPARE     = "PREPARE"
    final val PROMISE     = "PROMISE"
    final val ACCEPT      = "ACCEPT"
}

  object Payloads {
    case class RemoveStorageNode(storageNodeId:String)
    case class AddStorageNode(storageNodeId:String)
    case class Prepare(
                        operationId:Int,
                        guid:String,
                        timestamp:Long
                      )
    case class Propose(
                        guid:String,
                        url:String,
                        proposedElement: ProposedElement,
                        timestamp:Long,
                      )
    case class Promise(
                        guid:String,
                        timestamp:Long,
                        proposedElement: ProposedElement,
                        uploadUrl:String
                      )
    case class Accept(
                       guid:String,
                       url:String,
                       timestamp:Long,
                       proposedElement: ProposedElement
                     )

    case class PullDone(guid:String)
    case class Pull(
                       guid:String,
                       url:String,
                       userId:String,
                       bucketName:String,
                       compressionAlgorithm:String,
                       timestamp:Long,
                     )
  }
  case class StorageNode(poolId:String,nodeId:String)
  //  __________________________________________________________
//  trait NodeError extends Error
  case class TransactionNotFound(transactionId:String) extends NodeError {
    override def getMessage: String = s"TRANSACTION[$transactionId] not found"
  }
  case class BadArguments(message:String) extends NodeError{
    override def getMessage: String = message
  }
  case class DownloadError(message:String) extends NodeError{
    override def getMessage: String = s"DOWNLOAD_ERROR: $message"
  }
//  __________________________________________________________
case class UploadFileOutput(sink:File,isSlave:Boolean,metadata:FileMetadata)
//
//  case class RabbitContext(client:RabbitClient[IO],connection:AMQPConnection)
  case class NodeContextV5(
                            config: DefaultConfigV5,
                            logger: Logger[IO],
                            state:Ref[IO,NodeStateV5],
                            rabbitMQContext: RabbitMQContext
                          )


  case class ProposedElement(guid:String,hits:Int)
  object ProposedElement {
    def empty:ProposedElement = ProposedElement("",0)
  }
  case class CacheNodeProposal(proposedElement: ProposedElement,uploadUrl:String)
  case class CacheTransaction(
                               id:String,
                               nodeId:String,
                               cacheNodes:List[String],
                               proposedElements: Map[String,CacheNodeProposal]=Map.empty[String,CacheNodeProposal],
                               timestamp:Long,
                               proposedElement:ProposedElement,
                               data:fs2.Stream[IO,Byte],
                               userId:UUID,
                               bucketName:String,
                               filename:String,
                               compressionAlgorithm:compression.CompressionAlgorithm,
                               authedRequest: AuthedRequest[IO,User]
                             )
  case class RequestX(operationId:Int,authedRequest: AuthedRequest[IO,User])
  case class NodeStateV5(
                          status:Status,
                          cacheNodes: List[String] = List.empty[String],
                          loadBalancer: balancer.LoadBalancer,
                          loadBalancerPublisher:PublisherV2,
                          keyStore:PublisherV2,
                          cacheNodePubs:Map[String,PublisherV2],
                          syncNodePubs:Map[String,PublisherV2],
                          syncLB:Balancer[String],
                          ip:String = "127.0.0.1",
                          availableResources:Int,
                          freeStorageSpace:Long,
                          usedStorageSpace:Long,
                          replicationStrategy:String,
                          cache: MemoryCache[IO,String,Int],
                          currentEntries:Ref[IO,List[String]],
                          cacheSize:Int,
                          downloadCounter:Int=0,
                          transactions:Map[String,CacheTransaction]= Map.empty[String,CacheTransaction],
                          queue:Queue[IO,RequestX],
                          currentOperationId:Option[Int],
//                          status:
                      )
}
