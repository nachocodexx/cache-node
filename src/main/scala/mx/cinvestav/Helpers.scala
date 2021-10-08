package mx.cinvestav
import cats.effect.kernel.Outcome

import java.io.{ByteArrayOutputStream, File}
import java.net.URL
import cats.implicits._
import cats.effect.{IO, Ref}
import com.github.gekomad.scalacompress.CompressionStats
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpMessage, AmqpProperties, ExchangeName, ExchangeType, RoutingKey}
import fs2.Pipe
import fs2.io.file.Files
import mx.cinvestav.Declarations.{CacheTransaction, CommandIds, DownloadError, NodeContextV5, ObjectS, Payloads, ProposedElement, StorageNode, User, liftFF}
import mx.cinvestav.cache.cache.{CachePolicy, EvictedItem}
import mx.cinvestav.cache.CacheX.{EvictedItem => EvictedItemV2}
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.commons.types.{Location, ObjectLocation, ObjectMetadata}
//import mx.cinvestav.server.HttpServer.User
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
import mx.cinvestav.commons.security.SecureX
import at.favre.lib.bytes.Bytes
import mx.cinvestav.commons.stopwatch.StopWatch._
object Helpers {

//  Update storage capacity cache
  def updateStorage(evictedItem: EvictedItemV2[ObjectS],level1ObjectSize:Long)(implicit ctx:NodeContextV5 ): IO[Unit] = {
    for {
      _                      <-ctx.state.update( s=>{
        val evictedItemSize = evictedItem.value.metadata.getOrElse("objectSize","0").toLong
        s.copy(
          usedStorageSpace = (s.usedStorageSpace - evictedItemSize) + level1ObjectSize,
          availableStorageSpace = (s.availableStorageSpace +evictedItemSize) - level1ObjectSize
        )
      }
      )
    } yield ()
  }
  //
  def streamBytesToBuffer:Pipe[IO,Byte,ByteArrayOutputStream] = s0 =>
    fs2.Stream.suspend{
      s0.chunks.fold(new ByteArrayOutputStream(1000)){ (buffer,chunk)=>
        val bytes = chunk.toArraySlice
        buffer.write(bytes.values,bytes.offset,bytes.size)
        buffer
      }
    }


  def evictedItemRedirectTo(req:Request[IO],oneURL:String,evictedElem:EvictedItemV2[ObjectS],user: User)(implicit ctx:NodeContextV5): IO[Response[IO]] = for {
    _ <- IO.unit
    body   = fs2.Stream.emits(evictedElem.value.bytes).covary[IO]
//    body     = buffer.flatMap(buffer=>Stream.emits(buffer.toByteArray))
//    bodyLen  <- buffer.map(_.size()).compile.last
//    _ <- ctx.logger.debug(s"EVICTED_ITEM_BYTES $bodyLen")
    multipart = Multipart[IO](
      parts = Vector(
        Part[IO](
          headers =  Headers(
            Header.Raw(CIString("guid"),evictedElem.key),
            Header.Raw(org.http4s.headers.`Content-Length`.name,evictedElem.value.metadata.getOrElse("objectSize", "0"))
          ),
          body = body
        )
      )
    )
    newUri    = Uri.unsafeFromString(oneURL).withPath(req.uri.path)
    //            REQUEST
    newReq    = Request[IO](
      method  = org.http4s.Method.POST,
      uri     = newUri,
      headers = multipart.headers,
      httpVersion = req.httpVersion
    ).withEntity(multipart).putHeaders(Headers(
      Header.Raw(CIString("User-Id"),user.id.toString),
      Header.Raw(CIString("Bucket-Id"),user.bucketName),
    ))
    levelOneResponse        <- Helpers.redirectTo(oneURL,newReq)

  } yield levelOneResponse
  def redirectTo(nodeUrl:String,req:Request[IO]): IO[Response[IO]] = for {
    _                   <- IO.unit
    newReq             = req.withUri(Uri.unsafeFromString(nodeUrl).withPath(req.uri.path))
    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    response           <- client.toHttpApp.run(newReq)
    _ <- finalizer
  } yield response

  def sendPull(userId:String,
               bucketName:String,
               evictedKey: String,
               syncNodePub:PublisherV2,
               evictedItemPath:java.nio.file.Path,
                 )(implicit ctx:NodeContextV5) = for {
    timestamp    <- IO.realTime.map(_.toMillis)
    currentState <- ctx.state.get
    client     = ctx.rabbitMQContext.client
    connection = ctx.rabbitMQContext.connection
    ip         = currentState.ip
    port       = ctx.config.port
    nodeId     = ctx.config.nodeId
    url        = s"http://$ip:$port/download/${evictedKey}"
    //    syncNodes  = currentState.cacheNodePubs.filter(_._1==replyTo).values.toList
    msgPayload = Payloads.Pull(
      guid      = evictedKey,
      timestamp = timestamp,
      url =url,
      userId=userId,
      bucketName =bucketName,
      compressionAlgorithm="LZ4",
      evictedItemPath = evictedItemPath.toString
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
                  evicted:EvictedItem,
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
//    GET CURRENT SATE
    currentState         <- ctx.state.get
//   GET CURRENT TIME
    timestamp            <- IO.realTime.map(_.toMillis)
//
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
//      HEADERS
        partHeaders      = part.headers
        guid             = partHeaders.get(key =ci"guid").map(_.head).map(_.value)
//          .map(UUID.fromString)
          .getOrElse(UUID.randomUUID().toString)
        contentType      = partHeaders.get(ci"Content-Type").map(_.head.value).getOrElse( "application/octet-stream")
        _ <- ctx.logger.debug("BEFORE_CHECKSUM")
        contentLength    = partHeaders.get(ci"Content-Length").flatMap(_.head.value.toIntOption)
//
        originalFilename = part.filename.getOrElse("default")
        originalMetadata = FileMetadata.fromPath(Paths.get(originalFilename))
        _                <- ctx.logger.debug(originalMetadata.toString)
        originalName     = part.name.getOrElse("default")
        //
        sinkPath         =  Paths.get(baseStr+s"/$guid")
        stream           = part.body
//        rawBytes         <- stream.compile.to(Array)

        checksumInfoFiber         <- stream.through(fs2.hash.sha512)
          .compile.to(Array)
          .map(Bytes.from(_).encodeHex(false))
          .stopwatch
          .start
        writeFile        = stream.through(Files[IO].writeAll(path=sinkPath)).compile.drain
//
        metadata         = ObjectMetadata(
          guid =  guid,
          size  = 0L,
          compression = ca.token,
          bucketName = bucketName,
          filename = originalFilename,
          name = originalName,
          locations = ObjectLocation(
            nodeId = nodeId,
            poolId = poolId,
            url = baseUrl+s"/download/$guid"
          )::Nil,
          policies = Nil,
          checksum = "",
          userId = userId.toString,
          //              .getOrElse(UUID.randomUUID().toString),
          version = 1,
          timestamp = timestamp,
          contentType = contentType,
          extension = originalMetadata.extension.value
        )
        //
        putRes           <- cacheX.putv2(cache,guid.toString)
        //      _____________________________________________________________________
        _                <- putRes.evicted match {
//         CACHE IS FULL
          case Some(element) =>  for {
            _             <-  ctx.logger.debug("CONSENSUS_INIT")
            _cacheNodes   =   cacheNodes.filter(_._1!=nodeId)
            cachePubs     =  _cacheNodes.values.toList
            _cacheNodeIds =  _cacheNodes.keys.toList
            _             <- ctx.logger.debug("CACHE_NODES "+_cacheNodeIds.mkString("//"))
            chordNode     =  ctx.config.keyStore.nodes.head
            putStatus     <- chordNode.put(guid,metadata.asJson.noSpaces)
            _             <- ctx.logger.debug(s"PUT_STATUS $putStatus")
            //            Transaction
            transactionId = guid
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
//         CACHE HAS EMPTY SLOTS
          case None => for {
            checksum <- checksumInfoFiber.join.flatMap {
              case Outcome.Succeeded(fa) => for {
                value <- fa
                _     <- ctx.logger.info(s"CHECKSUM $guid ${value.duration.toMillis}")
              } yield value.result
              case Outcome.Errored(e) => ctx.logger.error(e.getMessage) *> "NO_CHECKSUM".pure[IO]
              case Outcome.Canceled() => ctx.logger.error("CANCELLED") *> "NO_CHECKSUM".pure[IO]
            }

//            buffer     = new ByteArrayOutputStream()
//            rawBytes  <- stream.chunkN(8192).fold(buffer) { (buffer, chunk) =>
//                buffer.write(chunk.toArray)
//                buffer
//            }.compile.lastOrError
//            _ <- ctx.logger.debug(s"BUFFER ${buffer.size()}")
//                .compile.to(Array)
            _           <- writeFile
            _           <- compression.compress(ca=ca,source = sinkPath.toString,baseStr).value.flatMap {
              case Left(e) => ctx.logger.error(e.getMessage)
              case Right(value) => for {
                _           <- ctx.logger.debug(value.toString)
                _ <- ctx.logger.info(s"COMPRESSION $guid ${value.sizeIn} ${value.sizeOut} ${value.millSeconds}")
                newMetadata = metadata.copy(
                  checksum=checksum,
                  size = value.sizeOut
                )
                _ <- ctx.config.keyStore.nodes.head.put(guid.toString,newMetadata.asJson.noSpaces).start
//                _ <- ctx.logger.debug(keyStoreStatus.toString)
                _           <- ctx.logger.debug(newMetadata.asJson.toString)
                _           <- ctx.state.update(_.copy(status=StatuX.Up,currentOperationId = None))
                _           <- NIOFIles[IO].delete(sinkPath)
                endTime     <- IO.realTime.map(_.toMillis)
                duration    = endTime-timestamp
                _           <- ctx.logger.info(s"UPLOAD $guid ${value.sizeOut} $duration")
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
        Header.Raw(CIString("Compression-Algorithm"),"lz4"),
        Header.Raw(CIString("File-Decompress"),"false")
      )
    )
    status <- client.status(req)
    _ <- ctx.logger.debug(s"STATUS $status")
    _                  <- finalizer
  } yield ()

  //  ________________________________________________________________________________
//  def replicationCompleted(taskId:String,chunkId:String,location:Location)(implicit ctx:NodeContextV5) = for {
//
//    currentState <- ctx.state.get
//    lb           = currentState.loadBalancerPublisher
//    timestamp    <- IO.realTime.map(_.toMillis)
//    nodeId       = ctx.config.nodeId
//    properties   = AmqpProperties(headers = Map("commandId" -> StringVal("REPLICATION_COMPLETED") ))
//
//    msgPayload = PAYLOADS.ReplicationCompleted(
//      replicationTaskId = taskId,
//      storageNodeId     = nodeId,
//      chunkId           = chunkId,
//      //        uploadFileOutput.metadata.filename.value,
//      timestamp         = timestamp,
//      location          = location
//      //        Location(url=url,source =source.toString)
//    ).asJson.noSpaces
//    msg      = AmqpMessage[String](payload = msgPayload,properties = properties)
//    _ <- lb.publish(msg)
//  } yield ()

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

//  WRITE AND COMPRESS
 def writeThenCompress(guid:String,
                       ca:compression.CompressionAlgorithm,
                       stream:Stream[IO,Byte],
                       basePath:java.nio.file.Path,
                       sinkPath:java.nio.file.Path)(implicit ctx:NodeContextV5): IO[CompressionStats] =
   for {
   _           <- stream.through(Files[IO].writeAll(sinkPath)).compile.drain
   res           <- compression.compress(ca=ca,source = sinkPath.toString,basePath.toString).value.flatMap {
     case Left(e) => ctx.logger.error(e.getMessage) *> IO.raiseError(e)
     case Right(value) => for {
       _  <- NIOFIles[IO].delete(sinkPath)
     } yield value
   }
 }yield res

}
