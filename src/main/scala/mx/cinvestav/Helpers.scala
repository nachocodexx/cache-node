package mx.cinvestav
import java.io.File
import java.net.URL
import cats.implicits._
import cats.effect.{IO, Ref}
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpMessage, AmqpProperties, ExchangeName, ExchangeType, RoutingKey}
import fs2.io.file.Files
import mx.cinvestav.Declarations.{CacheTransaction, CommandIds, DownloadError, NodeContextV5, Payloads, ProposedElement, StorageNode, liftFF}
import mx.cinvestav.cache.cache.{CachePolicy, EvictedItem}
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.commons.types.{Location, ObjectMetadata}
import mx.cinvestav.server.HttpServer.User
import mx.cinvestav.server.Routes.UploadResponse
import mx.cinvestav.utils.v2.{PublisherConfig, PublisherV2, RabbitMQContext}
import org.http4s.Request
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci._
//
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.ExecutionContext.global
//
import io.circe.syntax._
import io.circe.generic.auto._
import scala.language.postfixOps
import mx.cinvestav.utils.v2.encoders._
import org.apache.commons.io.FileUtils
import mx.cinvestav.commons.payloads.{v2=>PAYLOADS}
import org.http4s._
import fs2.Stream
import cats.nio.file.{Files => NIOFIles}
//
import mx.cinvestav.commons.{status=>StatuX}
import mx.cinvestav.utils.v2.encoders._
object Helpers {

  def sendPull(userId:String,bucketName:String,evicted: EvictedItem,
                 syncNodePub:PublisherV2
                 )(implicit ctx:NodeContextV5) = for {
    timestamp    <- IO.realTime.map(_.toMillis)
    currentState <- ctx.state.get
    client     = ctx.rabbitMQContext.client
    connection = ctx.rabbitMQContext.connection
    ip         = currentState.ip
    port       = ctx.config.port
    nodeId     = ctx.config.nodeId
    url        = s"http://$ip:$port/download/${evicted.key}"
    //    syncNodes  = currentState.cacheNodePubs.filter(_._1==replyTo).values.toList
    msgPayload = Payloads.Pull(
      guid      = evicted.key,
      timestamp = timestamp,
      url =url,
      userId=userId,
      bucketName =bucketName,
      compressionAlgorithm="lz4",
    ).asJson.noSpaces

    properties  = AmqpProperties(
      headers = Map("commandId" -> StringVal(CommandIds.PULL)),
      replyTo = nodeId.some
    )
    msg      = AmqpMessage[String](payload = msgPayload, properties = properties)
    (channel,finalizer) <-  client.createChannel(connection).allocated
    implicit0(_channel:AMQPChannel) <- IO.pure(channel)
    _ <- syncNodePub.publishWithChannel(msg)
    _ <- finalizer
    _<- ctx.logger.debug(s"SENT TO ${syncNodePub.pubId}")
//    _ <- finalizer
    //
  } yield ()


  def sendPromise(nodeId:String,
                  payload: Payloads.Prepare,
//                  uploadUrl:String,
                  evicted: EvictedItem,
                  replyTo:String
                 )(implicit ctx:NodeContextV5) = for {
    timestamp    <- IO.realTime.map(_.toMillis)
    currentState <- ctx.state.get
    client     = ctx.rabbitMQContext.client
    connection = ctx.rabbitMQContext.connection
    ip         = currentState.ip
    port        = ctx.config.port
    cacheNodes  = currentState.cacheNodePubs.filter(_._1==replyTo).values.toList
    msgPayload = Payloads.Promise(
      guid      = payload.guid,
      timestamp = timestamp,
      proposedElement = ProposedElement(evicted.key,evicted.value),
      uploadUrl = s"http://$ip:$port/uploadv2"
    ).asJson.noSpaces
    properties  = AmqpProperties(
      headers = Map("commandId" -> StringVal(CommandIds.PROMISE)),
      replyTo = nodeId.some
    )
    msg      = AmqpMessage[String](payload = msgPayload, properties = properties)
    (channel,finalizer) <-  client.createChannel(connection).allocated
    implicit0(_channel:AMQPChannel) <- IO.pure(channel)
    _ <- cacheNodes.traverse { pub =>
      pub.publishWithChannel(msg)
    }
               _ <- finalizer
    //
  } yield ()

  //        uploadUrl = s"http://$ip:$port/uploadv2"

  def onUp(operationId:Int,authReq: AuthedRequest[IO,User])(implicit ctx:NodeContextV5) = for {
    currentState         <- ctx.state.get
    timestamp            <- IO.realTime.map(_.toMillis)
    nodeId               = ctx.config.nodeId
    poolId               = ctx.config.poolId
    port                 = ctx.config.port
    storagePath          =  ctx.config.storagePath
    ip                   = currentState.ip
    baseUrl              = s"http://${ip}:${port}"
    cacheNodes           = currentState.cacheNodePubs
    //    REQUEST
    user                 = authReq.context
    req                  = authReq.req
    payload              <- req.as[Multipart[IO]]
    //   CACHE
    cacheX               = CachePolicy(ctx.config.cachePolicy)
    //
    parts                = payload.parts
    headers              = req.headers
    compressionAlgorithm = headers.get(CIString("Compression-Algorithm")).map(_.head).map(_.value).map(_.toUpperCase()).getOrElse("")
    //
    currentOperationId   = currentState.currentOperationId
    _                    <- ctx.state.update(_.copy(currentOperationId = operationId.some ))
    //
    ca                   = compression.fromString(compressionAlgorithm)
    userId               =  user.id
    bucketName           =  user.bucketName
    //
    baseStr              =  s"$storagePath/$nodeId/$userId/$bucketName"
    basePath             =  Paths.get(baseStr)
    _                    <- NIOFIles[IO].createDirectories(basePath)
    _                    <- parts.traverse{ part=>
      for {
        _                <- IO.unit
        cache            = currentState.cache
        _                <- part.headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
        partHeaders      = part.headers
        //         Metadata
        guid             = partHeaders.get(key =ci"guid").map(_.head).map(_.value).map(UUID.fromString).getOrElse(UUID.randomUUID())
        contentType      = partHeaders.get(ci"Content-Type").map(_.head.value).getOrElse( "application/octet-stream")
        originalFilename = part.filename.getOrElse("default")
        originalName     = part.name.getOrElse("default")
        checksum         = ""
        metadata         = ObjectMetadata(
          guid =  guid,
          size  = 0L,
          compression = ca.token,
          bucketName = bucketName,
          filename = originalFilename,
          name = originalName,
          locations = Nil,
          policies = Nil,
          checksum = checksum,
          userId = userId.toString,
          //              .getOrElse(UUID.randomUUID().toString),
          version = 1,
          timestamp = timestamp,
          contentType = contentType,
          extension = ""
        )
        //
        sinkPath         =  Paths.get(baseStr+s"/$guid")
        stream           = part.body
        writeFile        = stream.through(Files[IO].writeAll(path=sinkPath)).compile.drain
        //
        _                <- ctx.logger.debug("GUID "+guid.toString)
        putRes           <- cacheX.putv2(cache,guid.toString)
        //      _____________________________________________________________________
        _                <- putRes.evicted match {
          case Some(element) =>  for {
            _             <- ctx.logger.debug("CONSENSUS_INIT")
            _cacheNodes   = cacheNodes.filter(_._1!=nodeId)
            cachePubs     = _cacheNodes.values.toList
            _cacheNodeIds = _cacheNodes.keys.toList
            _             <- ctx.logger.debug("CACHE_NODES "+_cacheNodeIds.mkString("//"))
            //            Transaction
            transactionId = guid.toString
            //                UUID.randomUUID().toString
            transaction   = CacheTransaction(
              id = transactionId,
              cacheNodes =_cacheNodeIds,
              timestamp = timestamp,
              proposedElement = ProposedElement(element.key,element.value),
              nodeId = nodeId,
              data = stream,
              userId = userId,
              bucketName = bucketName,
              filename = FileMetadata.fromPath(Paths.get ( originalFilename) ).filename.value,
              compressionAlgorithm=ca,
              authedRequest=authReq
            )
            //            ADD_TRANSACTION_LOCAL
            _             <- ctx.state.update(s=>s.copy(transactions = s.transactions+ (transactionId->transaction)))
            //            SEND PREPARE TO CACHE_NODES
            payload       = Payloads.Prepare(
              operationId = operationId,
              guid = transactionId,
              timestamp=timestamp,
            ).asJson.noSpaces
            //
            properties = AmqpProperties(
              headers = Map("commandId" -> StringVal(CommandIds.PREPARE)  ),
              replyTo = nodeId.some
            )
            //
            msg        = AmqpMessage[String](payload=payload,properties=properties)
            //
            implicit0(rabbitMQContext:RabbitMQContext) <- IO.pure(ctx.rabbitMQContext)
            connection                      = rabbitMQContext.connection
            client                          = rabbitMQContext.client
            (channel,finalizer)             <- client.createChannel(connection).allocated
            implicit0(_channel:AMQPChannel) <- IO.pure(channel)
            _                               <- cachePubs.traverse(_.publishWithChannel(msg))
            _                               <- finalizer
//            _          <- ctx.logger.debug("INIT PAXOS")
          } yield ()
          case None => for {
            _        <- ctx.logger.debug(s"NO EVICTION $ca")
            _        <- writeFile
            _        <- ctx.logger.debug(s"AFTER_WRITE")
            _        <- compression.compress(ca=ca,source = sinkPath.toString,baseStr)
              .value.flatMap {
              case Left(e) => ctx.logger.error(e.getMessage)
              case Right(value) => for {
                _        <- ctx.logger.debug(value.toString)
                //                FREE NODE
                _        <- ctx.state.update(_.copy(status=StatuX.Up,currentOperationId = None))
                _        <- NIOFIles[IO].delete(sinkPath)
                endTime  <- IO.realTime.map(_.toMillis)
                duration = endTime-timestamp
                _        <- ctx.logger.info(s"UPLOAD $guid $duration")
              } yield ()
            }.onError{ t=> ctx.logger.error(t.getMessage)}
          } yield ()
        }
        //          UPDATE CACHE
        _                <- ctx.state.update(_.copy(cache=putRes.newCache))
        _ <- ctx.logger.debug("______________________________________________________________________")
      } yield ()
    }
    //
    endedAt   <- IO.realTime.map(_.toMillis)
    payload   = UploadResponse(operationId = UUID.randomUUID().toString ,uploadSize = 0L,duration = endedAt-timestamp )
    response  <- Ok("RESPONSE")

  } yield ()



  def sendRequest(req:Request[IO])(implicit ctx:NodeContextV5) = for {
    _ <- ctx.logger.debug("HRE(0)")
    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    _ <- ctx.logger.debug("HRE(1)")
//    _ <- ctx.logger.debug(req.uri.withPath(Uri.Path.unsafeFromString("")   ))
    newReq = Request[IO](
      req.method,
      uri=Uri.unsafeFromString("http://localhost:4000/stats/v2"),
      req.httpVersion,
      req.headers
    )
    _ <- ctx.logger.debug(req.toString)
    status <- client.status(newReq)
    _ <- ctx.logger.debug("HRE(2)")
    _ <- ctx.logger.debug(status.toString)
    _ <- ctx.logger.debug("HRE(3)")
    _ <- finalizer
  } yield ()

  def uploadToNode(filename:String,guid:String,userId:String,bucketName:String,url:String,body:Stream[IO,Byte])(implicit ctx:NodeContextV5)= for {
    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    multipart = Multipart[IO](
      parts = Vector(
        Part[IO](
          headers = Headers(
            Header.Raw(CIString("filename"),filename ),
            Header.Raw(CIString("guid"),guid)
          ),
          body    = body
        )
      )
    )
    req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(url),
      headers = multipart.headers
    )
      .withEntity(multipart)
      .putHeaders(
      Headers(
        Header.Raw(CIString("User-Id"),userId),
        Header.Raw(CIString("Bucket-Id"),bucketName),
        Header.Raw(CIString("Compression-Algorithm"),"lz4")
      )
    )
    status <- client.status(req)
    _ <- ctx.logger.debug(s"STATUS $status")
    _                  <- finalizer
  } yield ()

  //  ________________________________________________________________________________
  def replicationCompleted(taskId:String,chunkId:String,location:Location)(implicit ctx:NodeContextV5) = for {

    currentState <- ctx.state.get
    lb           = currentState.loadBalancerPublisher
    timestamp    <- IO.realTime.map(_.toMillis)
    nodeId       = ctx.config.nodeId
    properties   = AmqpProperties(headers = Map("commandId" -> StringVal("REPLICATION_COMPLETED") ))

    msgPayload = PAYLOADS.ReplicationCompleted(
      replicationTaskId = taskId,
      storageNodeId     = nodeId,
      chunkId           = chunkId,
      //        uploadFileOutput.metadata.filename.value,
      timestamp         = timestamp,
      location          = location
      //        Location(url=url,source =source.toString)
    ).asJson.noSpaces
    msg      = AmqpMessage[String](payload = msgPayload,properties = properties)
    _ <- lb.publish(msg)
  } yield ()

  def fromStorageNodeToPublisher(x:StorageNode)(implicit rabbitMQContext:RabbitMQContext): PublisherV2 = {
    val exchange = ExchangeName(x.poolId)
    val routingKey = RoutingKey(s"${x.poolId}.${x.nodeId}")
    val cfg = PublisherConfig(exchangeName = exchange,routingKey = routingKey)
    PublisherV2.create(x.nodeId,cfg)
  }

  def downloadFromURL(url:URL,destination: File): Either[DownloadError, Unit] = {
    try{
        Right(FileUtils.copyURLToFile(url,destination))
    } catch {
      case ex: Throwable => Left(DownloadError(ex.getMessage))
    }
  }

}
