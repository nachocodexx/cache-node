package mx.cinvestav.config

//
import scala.concurrent.ExecutionContext.global
import cats.effect._

import java.net.InetAddress
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.types.ObjectMetadata
import mx.cinvestav.Implicits._
import mx.cinvestav.commons.events.{Del, Evicted, Put}
import mx.cinvestav.commons.payloads.{AddCacheNode,PutAndGet}
//
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.{Header, Headers, MediaType, Method, Request, Status, Uri, headers}
import org.http4s.client.Client
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci.CIString
//
import fs2.Stream
import java.util.UUID

case class ChordGetResponse(
                             key:String,
                             value:ObjectMetadata,
                             url:String,
                             milliSeconds:Long
                           )
case class ChordPut(key:String,value:String)
case class ChordNode(nodeId:String,host:String,port:Int){
  def url = s"http://$host:$port"
  def put(key:String,value:String): IO[Status] = {
    val body = ChordPut(key,value)
//      Json.obj("key"->key.asJson,"value"->value.asJson)
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(url+"/put"),
      headers = Headers(org.http4s.headers.`Content-Type`(MediaType.application.json)),
    )
      .withEntity(body)
    BlazeClientBuilder[IO](global).resource.use{ client=>
      client.status(req)
    }
  }
  def get(key:String) = {
    val req   = Request[IO](
      method  = Method.GET,
      uri     = Uri.unsafeFromString(url+s"/get/$key"),
    )
    BlazeClientBuilder[IO](global).resource.use{ client =>
      client.expect[ChordGetResponse](req)
    }
  }
}
case class KeyStoreInfo(
                         nodes:List[ChordNode]
                       )


case class LoadBalancerInfo(
                                exchange:String,
                                routingKey:String,
                                ip:String,
                                port:Int
                           ){
  def httpURL = s"http://$ip:$port"
  def addNodeUri = s"http://$ip:$port/api/v6/add-node"
  def addNode(payload:AddCacheNode)(implicit ctx:NodeContext) = for {
    timestamp          <- IO.realTime.map(_.toMillis)
    _                  <- ctx.logger.debug("ADD_NODE")
    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    req                = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(this.addNodeUri)
    ).withEntity(payload.asJson)
      .putHeaders(
        Headers(Header.Raw(CIString("Timestamp"),timestamp.toString))
      )
    status             <- client.status(req)
    _                  <- ctx.logger.debug(s"STATUS $status")
    _                  <- finalizer
  }  yield ()
}
case class LoadBalancerLevel(zero: LoadBalancerInfo, one:LoadBalancerInfo,cloud:LoadBalancerInfo)

case class Pool(hostname:String,port:Int) {
  def httpURL = s"http://$hostname:$port"
  def addNodeUri = s"http://$hostname:$port/api/v2/add-node"
  def evictedUri = s"http://$hostname:$port/api/v2/evicted"
  def putUri = s"$httpURL/api/v2/put"
  def monirotingUrl(nodeId:String) = s"$httpURL/api/v7/monitoring/$nodeId"
  def uploadUri = s"$httpURL/api/v2/upload"
  def downloadUri(objectId:String) = s"$httpURL/api/v7/download/$objectId"

  def sendPut(put:PutAndGet)(implicit ctx:NodeContext) = for {
    _                  <- IO.unit
    req                = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(this.putUri)
    ).withEntity(put.asJson)
    status             <- ctx.client.status(req)
    _                  <- ctx.logger.debug(s"PUT_STATUS $status")
    //    _                  <- finalizer
  }  yield ()

  def upload(
              objectId:String,
              bytes:Array[Byte],
              userId:String,
              operationId:String = UUID.randomUUID().toString,
              contentType:MediaType= MediaType.text.plain
            )(implicit ctx:NodeContext) = {
    val multipart = Multipart[IO](
      parts = Vector(Part(
        headers = Headers(
          Header.Raw(CIString("Object-Id"),objectId ),
          headers.`Content-Type`(contentType),
          headers.`Content-Length`(bytes.length)
        ),
        body = Stream.emits(bytes).covary[IO]
      )
      )
    )
    val loadBalanceRequest = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(ctx.config.cachePool.uploadUri),
      headers = Headers(
          Header.Raw(CIString("Operation-Id"),operationId),
          Header.Raw(CIString("User-Id"),userId),
          Header.Raw(CIString("Bucket-Id"), "nacho-bucket" ),
          Header.Raw(CIString("Object-Id"),objectId),
          Header.Raw(CIString("Object-Size"),bytes.length.toString),
        )
    )
    val nodeUploadRequest = (uri:String)=>Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(uri),
      headers = multipart.headers
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("Operation-Id"),operationId),
          Header.Raw(CIString("User-Id"),userId),
          Header.Raw(CIString("Bucket-Id"), "nacho-bucket" ),
          Header.Raw(CIString("Object-Id"),objectId),
          Header.Raw(CIString("Object-Size"),bytes.length.toString),
        )
      )

    val app = for {
      response0 <- ctx.client.stream(loadBalanceRequest)
      headers0        = response0.headers
      alreadyUploaded = headers0.get(CIString("Already-Uploaded")).flatMap(_.head.value.toBooleanOption).getOrElse(false)
      res               <- if(!alreadyUploaded) {
        for {
//          _               <- Stream.eval(ctx.logger.debug("LB_STATUS "+response0.status.toString))
          _               <- Stream.eval(ctx.logger.debug("LB_RESPONSE"+response0.toString))
          y               <- Stream.eval(response0.body.compile.to(Array))
          nodeURL         = new String(y)
//          _               <- Stream.eval(ctx.logger.debug(s"UPLOAD_NODE_URL $nodeURL"))
          nodeReq         = nodeUploadRequest(nodeURL)
          res             <- ctx.client.stream(nodeReq)
          uploadTo        = res.headers.get(CIString("Node-Id")).map(_.head.value).getOrElse("")
          _               <- Stream.eval(ctx.logger.debug(s"UPLOAD_STATUS ${res.status}"))
        } yield (uploadTo,nodeURL)
      } else {
        for {
          _ <- Stream.eval(IO.unit)
        } yield ("","")
      }
//      _
//    } yield (uploadTo,nodeURL)
    } yield (res)
  app.compile.lastOrError
  }
  def download(objectId:String, objectSize:Long=0, userId:String="", operationId:String="", objectExtension:String="")(implicit ctx:NodeContext)={
    val downloadReq =(uri:String)=> Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(uri),
      headers = Headers(
        Header.Raw(CIString("Operation-Id"),operationId),
        Header.Raw(CIString("User-Id"),userId),
        Header.Raw(CIString("Bucket-Id"),"default"),
        Header.Raw(CIString("Object-Size"),objectSize.toString),
        Header.Raw(CIString("Object-Extension"),objectExtension),
      )
    )

    val app = for {
      response0 <- ctx.client.stream(downloadReq(downloadUri(objectId)))
      _         <- Stream.eval(ctx.logger.debug(response0.toString))
      y         <- Stream.eval(response0.body.compile.to(Array))
      nodeUrl   = new String(y)
//      nodeUrl   <- Stream.eval(response0.as[String].onError(e=>ctx.logger.debug(s"DECODER_ERROR ${e.getMessage}")))
      _         <- Stream.eval(ctx.logger.debug(s"DOWNLOAD_NODE_URL $nodeUrl"))
      response1 <- ctx.client.stream(downloadReq(nodeUrl))
      _         <- Stream.eval(ctx.logger.debug("DOWNLOAD_RES "+response1.toString))
      b         <- response1.body
//      body      <- Stream.eval(response1.body.compile.to(Array))
    } yield b
    val x = app.compile.to(Array) //      .lastOrError
    x
  }

  def sendEvicted(payload:Del)(implicit ctx:NodeContext) = for {
    timestamp          <- IO.realTime.map(_.toMillis)
//    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    evicted            = Evicted(
      serialNumber = payload.serialNumber,
      nodeId = payload.nodeId,
      objectId = payload.objectId,
      objectSize = payload.objectSize,
      fromNodeId = payload.nodeId,
      timestamp = payload.timestamp,
      serviceTimeNanos = payload.serviceTimeNanos,
      eventId = payload.eventId,
      monotonicTimestamp = 0,
      correlationId = payload.correlationId
    )
//    _ <- ctx.logger.debug(s"REQUES_URI ${this.evictedUri}")
    req                = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(this.evictedUri)
    ).withEntity(evicted.asJson)
    status             <- ctx.client.status(req)
    _                  <- ctx.logger.debug(s"EVICTED_STATUS $status")
//    _                  <- finalizer
  }  yield ()
  def addNode(client:Client[IO])(payload:AddCacheNode)(implicit ctx:NodeContext) = for {
    timestamp          <- IO.realTime.map(_.toMillis)
    req                = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"${this.httpURL}/api/v${ctx.config.apiVersion}/add-node")
    ).withEntity(payload.asJson)
      .putHeaders(
        Headers(Header.Raw(CIString("Timestamp"),timestamp.toString))
      )
    status             <- client.status(req)
    _                  <- ctx.logger.debug(s"STATUS $status")
//    _                  <- finalizer
  }  yield ()
}

case class ServiceReplicator(hostname:String,port:Int){
  def startNode()(implicit ctx:NodeContext) = {
    val nodeId  = ctx.config.nodeId
    val uri     = Uri.unsafeFromString(s"http://$hostname:$port/api/v${ctx.config.apiVersion}/nodes/start/$nodeId")
//    val ip      = InetAddress.getLocalHost.getHostAddress
//    val port    = InetAddress
    val request =  Request[IO](
      method = Method.POST,
      uri    = uri,
      headers = Headers.empty
    )
    ctx.client.status(request)
  }
}

case class DefaultConfigV5(
                            nodeId:String,
                            poolId:String,
                            storagePath:String,
                            cachePolicy:String,
                            cacheSize:Int,
                            port:Int,
                            host:String,
                            dropboxAccessToken:String,
                            totalStorageCapacity:Long,
                            level:Int=0,
                            pool:Pool,
                            cloudEnabled:Boolean=false,
                            monitoringDelayMs:Long,
                            experimentId:String,
                            inMemory:Boolean,
                            cachePool:Pool,
                            apiVersion:Int,
                            serviceReplicator: ServiceReplicator,
                            intervalMs:Long
                        )

