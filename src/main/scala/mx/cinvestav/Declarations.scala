package mx.cinvestav

import cats.data.EitherT
import cats.effect.std.Semaphore
import cats.effect.{IO, Ref}
import com.dropbox.core.v2.DbxClientV2
import io.chrisdavenport.mules.MemoryCache
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.{Del, ObjectHashing, Push, Pull => PullEvent, TransferredTemperature => SetDownloads}
import org.http4s.client.Client

import java.nio.file.Path
import mx.cinvestav.commons.events.{EventX, Get, Put}
import mx.cinvestav.commons.types
import mx.cinvestav.commons.types.{CompletedOperation, UploadHeaders}
import org.http4s.Headers
import org.typelevel.ci.CIString
//
import io.circe._
import io.circe.syntax._
//
import mx.cinvestav.commons.errors.NodeError
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.commons.status.Status
import mx.cinvestav.config.DefaultConfigV5
import mx.cinvestav.commons.compression
import org.http4s.AuthedRequest
import org.typelevel.log4cats.Logger
import java.io.File
import java.util.UUID
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object Declarations {
  //
   case class Ball(id:String,size:Long,metadata:Map[String,String]=Map.empty[String,String])


  object Implicits {
    implicit val completedOperationsEncoder:Encoder[CompletedOperation] = {
      case dc: types.DownloadCompleted => dc.asJson
      case uc: types.UploadCompleted => uc.asJson
      case _ => Json.Null
    }
    implicit val eventXEncoder: Encoder[EventX] = {
      case put:Put => put.asJson
      case get:Get => get.asJson
      case del:Del => del.asJson
      case pull:PullEvent => pull.asJson
      case push:Push => push.asJson
      case transferredTemperature:SetDownloads => transferredTemperature.asJson
      case transferredTemperature:ObjectHashing => transferredTemperature.asJson
      case _ => Json.Null
    }
//  implicit val objectSEncoder: (String)=>Encoder[CacheItem[ObjectS]] = policy => (a: CacheItem[ObjectS]) => Json.obj(
//    ("guid" -> a.value.guid.asJson),
//    if (policy == "LFU") ("hits" -> a.counter.asJson) else ("sequence_number" -> a.counter.asJson),
//    ("metadata" -> a.value.metadata.asJson)
//  )
  implicit val objectSEncoderv2: Encoder[ObjectS] = (o: ObjectS) => Json.obj(
    ("guid" -> o.guid.asJson),
    ("size"-> o.bytes.length.asJson),
    ("metadata"->o.metadata.asJson)
  )
  implicit val objectDEncoder:Encoder[ObjectD] = (o:ObjectD) => Json.obj(
    ("guid" -> o.guid.asJson),
    "path"-> o.path.toString.asJson,
    ("metadata"->o.metadata.asJson)
  )
  implicit val iObjectEncoder:Encoder[IObject] = {
    case o: ObjectD => o.asJson(objectDEncoder)
    case o: ObjectS => o.asJson(objectSEncoderv2)
  }
  }
  case class ObjectX(guid:String,bytes:Array[Byte],metadata:Map[String,String])

  trait IObject{
    def guid:String
    def metadata:Map[String,String]
  }
  case class ObjectD(guid:String, path:Path, metadata: Map[String,String]) extends IObject
  case class ObjectS(guid:String, bytes: Array[Byte], metadata:Map[String,String]) extends IObject

  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           serviceTimeNanos:Long,
                           timestamp:Long,
                           level:Int
                         )
  //
//
  case class User(id:String,bucketName:String)
//
  def liftFF[A]: IO[A] => EitherT[IO, NodeError, A] =  commons.liftFF[A,NodeError]
//
//  object CommandIds {
//    final val ADD_NODE    = "ADD_NODE"
//    final val REMOVE_NODE = "REMOVE_NODE"
//    final val REPLICATE     = "REPLICATE"
//    final val PULL     = "PULL"
//  final val PULL_DONE     = "PULL_DONE"
//  //
//    final val PROPOSE     = "PROPOSE"
//    final val PREPARE     = "PREPARE"
//    final val PROMISE     = "PROMISE"
//    final val ACCEPT      = "ACCEPT"
//}

//  object Payloads {
//    case class RemoveStorageNode(storageNodeId:String)
//    case class AddStorageNode(storageNodeId:String)
//    case class Prepare(
//                        operationId:Int,
//                        guid:String,
//                        timestamp:Long
//                      )
//    case class Propose(
//                        guid:String,
//                        url:String,
//                        proposedElement: ProposedElement,
//                        timestamp:Long,
//                      )
//    case class Promise(
//                        guid:String,
//                        timestamp:Long,
//                        proposedElement: ProposedElement,
//                        uploadUrl:String
//                      )
//    case class Accept(
//                       guid:String,
//                       url:String,
//                       timestamp:Long,
//                       proposedElement: ProposedElement
//                     )
//
//    case class PullDone(guid:String,evictedItemPath:String,timestamp:Long)
//    case class Pull(
//                       guid:String,
//                       url:String,
//                       userId:String,
//                       bucketName:String,
//                       compressionAlgorithm:String,
//                       evictedItemPath:String,
//                       timestamp:Long,
//                     )
//  }
//  case class StorageNode(poolId:String,nodeId:String)
  //  __________________________________________________________
//  trait NodeError extends Error
//  case class TransactionNotFound(transactionId:String) extends NodeError {
//    override def getMessage: String = s"TRANSACTION[$transactionId] not found"
//  }
//  case class BadArguments(message:String) extends NodeError{
//    override def getMessage: String = message
//  }
//  case class DownloadError(message:String) extends NodeError{
//    override def getMessage: String = s"DOWNLOAD_ERROR: $message"
//  }
////  __________________________________________________________
//case class UploadFileOutput(sink:File,isSlave:Boolean,metadata:FileMetadata)
////
//  case class RabbitContext(client:RabbitClient[IO],connection:AMQPConnection)

  case class NodeContext(
                            config: DefaultConfigV5,
                            logger: Logger[IO],
                            errorLogger:Logger[IO],
                            state:Ref[IO,NodeStateV6],
                            client:Client[IO],
                            initTime:Long = 0L
                          )
//  case class NodeContextV5(
//                            config: DefaultConfigV5,
//                            logger: Logger[IO],
//                            state:Ref[IO,NodeStateV5],
////                            rabbitMQContext: RabbitMQContext
//                          )


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
  case class CacheNode(
                        nodeId:String,
                        ip:Option[String]=None,
                        port:Option[Int]=None
                      )

  case class NodeStateV6(
                          levelId:String,
                          status:Status,
                          ip:String = "127.0.0.1",
                          totalStorageSpace:Long=1000000000,
                          balls:List[Ball]=Nil,
                          completedOperations:List[CompletedOperation]=Nil,
                          cache: MemoryCache[IO,String,IObject],
                          cacheSize:Int,
                          downloadCounter:Int=0,
                          dropboxClient:DbxClientV2,
                          events:List[EventX] =Nil,
//                          s:Semaphore[IO],
                          experimentId:String,
                          metadata:Map[String,ObjectD] = Map.empty[String,ObjectD],
                          currentOperation:Int
                        )

  object UploadHeadersOps {
    def fromHeaders(headers:Headers)(implicit ctx:NodeContext) = {
      for {
        serviceTimeStart     <- IO.monotonic.map(_.toNanos)
        //     ________________________________________________________________
        operationId             = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        clientId                = headers.get(CIString("Client-Id")).map(_.head.value).getOrElse("CLIENT_ID")
        objectId                = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        objectSize              = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        fileExtension           = headers.get(CIString("File-Extension")).map(_.head.value).getOrElse("")
        filePath                = headers.get(CIString("File-Path")).map(_.head.value).getOrElse(s"$objectId.$fileExtension")
        compressionAlgorithm    = headers.get(CIString("Compression-Algorithm")).map(_.head.value).getOrElse("")
        requestStartAt          = headers.get(CIString("Request-Start-At")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
        catalogId               = headers.get(CIString("Catalog-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        digest                  = headers.get(CIString("Digest")).map(_.head.value).getOrElse("DIGEST")
        blockIndex              = headers.get(CIString("Block-Index")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
        blockTotal              = headers.get(CIString("Block-Total")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
        arrivalTime             = headers.get(CIString("Arrival-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
        collaborative           = headers.get(CIString("Collaborative")).map(_.head.value).flatMap(_.toBooleanOption).getOrElse(false)
        replicationTechnique    = headers.get(CIString("Replication-Technique")).map(_.head.value).getOrElse("ACTIVE")
        replicationTransferType = headers.get(CIString("Replication-Transfer-Type")).map(_.head.value).getOrElse("PUSH")
        replicaNodes            = headers.get(CIString("Replica-Node")).map(_.map(_.value).toList).getOrElse(Nil)
        pivotReplicaNode        = headers.get(CIString("Pivot-Replica-Node")).map(_.head.value).getOrElse("PIVOT_REPLICA_NODE")
        replicationFactor       = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
        _blockId                = s"${objectId}_${blockIndex}"
        blockId                 = headers.get(CIString("Block-Id")).map(_.head.value).getOrElse(_blockId)
        //      __________________________________________________________________________________________________________________
        upheaders               = UploadHeaders(
          operationId             = operationId,
          objectId                = objectId,
          objectSize              = objectSize,
          fileExtension           = fileExtension,
          filePath                = filePath,
          compressionAlgorithm    = compressionAlgorithm,
          requestStartAt          = requestStartAt,
          catalogId               = catalogId,
          digest                  = digest,
          blockIndex              = blockIndex,
          blockTotal              = blockTotal,
          arrivalTime             = arrivalTime,
          collaborative           = collaborative,
          replicaNodes            = replicaNodes,
          replicationTechnique    = replicationTechnique,
          replicationTransferType = replicationTransferType,
          blockId                 = blockId,
          clientId                = clientId,
          pivotReplicaNode        = pivotReplicaNode,
          replicationFactor       = replicationFactor,
          correlationId           = ""
        )
      } yield upheaders
    }
  }
}
