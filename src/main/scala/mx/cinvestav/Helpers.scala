package mx.cinvestav
import cats.effect.kernel.Outcome

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.net.URL
import cats.implicits._
import cats.effect.{IO, Ref}
import com.github.gekomad.scalacompress.CompressionStats
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpMessage, AmqpProperties, ExchangeName, ExchangeType, RoutingKey}
import fs2.Pipe
import fs2.io.file.Files
import mx.cinvestav.Declarations.{CacheTransaction, CommandIds, DownloadError, NodeContextV5, NodeContextV6, ObjectS, Payloads, ProposedElement, StorageNode, User, liftFF}
import mx.cinvestav.cache.cache.{CachePolicy, EvictedItem}
import mx.cinvestav.cache.CacheX.{EvictedItem => EvictedItemV2}
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.events.{Del, EventX, Get, Push, Put, Pull => PullEvent}
import mx.cinvestav.events.Events
import mx.cinvestav.server.RouteV6.PushResponse
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


  def putInCacheGeneric(guid:String, value:ObjectS,newEvents:Option[EvictedItemV2[ObjectS]] => List[EventX] )(implicit ctx:NodeContextV6): IO[Response[IO]] = {

    for {
      arrivalTime   <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      currentLevel  = ctx.config.level
      currentNodeId = ctx.config.nodeId
      cache         = currentState.cacheX
      levelId       = currentState.levelId
      objectSize    = value.bytes.length
      putResponse <- cache.put(key=guid,value=value)

      _ <- ctx.state.update(
        s=>{
          s.copy(
            events = s.events ++ newEvents(putResponse.evictedItem)
          )
        }
      )
      response    <- putResponse.evictedItem match {
        case Some(evictedElement) =>  for {
          endAt0     <- IO.realTime.map(_.toMillis)
          //            get service time
          putServiceTime      = endAt0 - arrivalTime
          //        add event
          _          <- ctx.logger.info(s"PUT $guid $levelId $putServiceTime")
          //              Update storage of the node
          // SENT TO CLOUD
          evictedObjectExt = evictedElement.value.metadata.getOrElse("extension","bin")
          filename = s"${evictedElement.key}.$evictedObjectExt"

          pushToCloud = for{

            pushTimestamp      <- IO.realTime.map(_.toMillis)
            rawEvents          <- ctx.state.get.map(_.events)
            events             =  Events.relativeInterpretEvents(events = rawEvents)
            alreadyPushToCloud = Events.alreadyPushedToCloud(objectId = evictedElement.key , events = events)
            _                   <- ctx.logger.debug(s"ALREADY_PUSH_TO_CLOUD[${evictedElement.key}] $alreadyPushToCloud")
            _                  <- if(alreadyPushToCloud) IO.unit
            else {
              for {
                metadata           <- Dropbox.uploadObject(currentState.dropboxClient)(
                  filename = filename,
                  in = new ByteArrayInputStream(evictedElement.value.bytes)
                )
                _ <- ctx.logger.debug(s"DROPBOX_METADATA $metadata")
                //            get timestamp
                endAt     <- IO.realTime.map(_.toMillis)
                //            get service time
                time      = endAt - arrivalTime
                _ <- ctx.logger.info(s"PUT ${evictedElement.key} CLOUD $time")
                pushEvent = Push(
                  eventId = UUID.randomUUID().toString,
                  serialNumber = 0,
                  nodeId = currentNodeId,
                  objectId = evictedElement.key,
                  objectSize = evictedElement.value.bytes.length,
                  pushTo = "Dropbox",
                  timestamp = pushTimestamp,
                  milliSeconds = time
                )
                _ <- ctx.state.update(s=>s.copy(events = s.events :+ pushEvent.copy(serialNumber = s.events.length)))
              } yield ()
            }
            //            metadata <- Dropbox.uploadObject(currentState.dropboxClient)(
//              filename = filename,
//              in = new ByteArrayInputStream(evictedElem.value.bytes)
//            )
//            _ <- ctx.logger.debug(s"DROPBOX_METADATA $metadata")
//            //            get timestamp
//            endAt     <- IO.realTime.map(_.toMillis)
//            //            get service time
//            time      = endAt - arrivalTime
//            _ <- ctx.logger.info(s"PUT ${evictedElem.key} CLOUD $time")
          } yield ()

          _ <- pushToCloud.start
          responsePayload         = PushResponse(
            userId= "USER_ID",
            guid=guid,
            objectSize=  value.bytes.length,
            milliSeconds = putServiceTime,
            timestamp = arrivalTime,
            level  = ctx.config.level,
            nodeId = ctx.config.nodeId
          )
          //
          response <- Ok(responsePayload.asJson,
            Headers(
              Header.Raw(CIString("Evicted-Object-Id"),evictedElement.key),
              Header.Raw(CIString("Evicted-Object-Size"),evictedElement.value.bytes.length.toString)
            )
          )
          //
        } yield response
        case None =>  for{
          endAt    <- IO.realTime.map(_.toMillis)
          time     = endAt - arrivalTime
          responsePayload = PushResponse(
            userId= "USER_ID",
            guid=guid,
            objectSize=  objectSize,
            milliSeconds = time,
            timestamp = endAt,
            level  = ctx.config.level,
            nodeId = ctx.config.nodeId
          )
          //        add event
          response <- Ok(responsePayload.asJson)
          _ <- ctx.logger.info(s"PUT $guid $levelId $time")
          //              else ctx.logger.info("")
        } yield response
      }
    } yield response
  }

  def putInCacheDownload(arrivalTime:Long,guid:String,value:ObjectS,pullEvent: PullEvent)(implicit ctx:NodeContextV6) = {
    for {
//      arrivalTime   <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      currentLevel  = ctx.config.level
      currentNodeId = ctx.config.nodeId
      cache         = currentState.cacheX
      objectSize    = value.bytes.length
      startPutServiceTime <- IO.realTime.map(_.toMillis)
      putResponse <- cache.put(key= guid,value = value)

      newHeaders <- putResponse.evictedItem match {
        case Some(evictedElement) => for {
          _ <- ctx.logger.debug("DOWNLOAD_EVICTION")
          putServiceTime <- IO.realTime.map(_.toMillis).map( _ - startPutServiceTime)
//        ___________________________________________________________________________________
          pushToCloud = for{
            pushTimestamp      <- IO.realTime.map(_.toMillis)
            rawEvents          <- ctx.state.get.map(_.events)
            events             =  Events.relativeInterpretEvents(events = rawEvents)
            alreadyPushToCloud = Events.alreadyPushedToCloud(objectId = evictedElement.key , events = events)
//            _                   <- ctx.logger.debug(s"ALREADY_PUSH_TO_CLOUD[${evictedElement.key}] $alreadyPushToCloud")
            _                  <- if(alreadyPushToCloud) IO.unit
            else {
               for {
//                 _ <- ctx.logger.debug("BEFORE_PUSH")
                 _        <- IO.unit
                 filename = s"${evictedElement.key}.${evictedElement.value.metadata.getOrElse("extension","")}"
                 metadata           <- Dropbox.uploadObject(currentState.dropboxClient)(
                   filename = filename,
                   in = new ByteArrayInputStream(evictedElement.value.bytes)
                 ).onError{ t=>
                   ctx.logger.error(t.getMessage)
                 }
//                 _ <- ctx.logger.debug("AFTER_PUSH")
//                 _ <- ctx.logger.debug(s"DROPBOX_METADATA $metadata")
                 //            get timestamp
                 endAt     <- IO.realTime.map(_.toMillis)
                 //            get service time
                 time      = endAt - arrivalTime
                 _ <- ctx.logger.info(s"PUT ${evictedElement.key} CLOUD $time")
                 _ <- ctx.state.update {
                   s =>
                     val newEvents = List(
                       pullEvent.copy(serialNumber = s.events.length),
                       Del(
                         eventId = UUID.randomUUID().toString,
                         serialNumber = s.events.length+1,
                         nodeId = currentNodeId,
                         objectId = evictedElement.key,
                         objectSize = evictedElement.value.bytes.length,
                         timestamp = pullEvent.timestamp+10,
                         milliSeconds = 1
                       ),
                       Put(
                         eventId = UUID.randomUUID().toString,
                         serialNumber = s.events.length+2,
                         nodeId = currentNodeId,
                         objectId = guid,
                         objectSize = objectSize,
                         timestamp = pullEvent.timestamp+20,
                         milliSeconds = putServiceTime
                       ),
                       Get(
                         eventId = UUID.randomUUID().toString,
                         serialNumber = s.events.length+3,
                         nodeId = currentNodeId,
                         objectId = guid,
                         objectSize = objectSize,
                         timestamp = pullEvent.timestamp+30,
                         milliSeconds = 1
                       ),
                       Push(
                         eventId = UUID.randomUUID().toString,
                         serialNumber = s.events.length + 4,
                         nodeId = currentNodeId,
                         objectId = evictedElement.key,
                         objectSize = evictedElement.value.bytes.length,
                         pushTo = "Dropbox",
                         timestamp = pushTimestamp,
                         milliSeconds = time
                       )
                     )
                     s.copy(
                       events = s.events ++ newEvents
                     )
                 }
               } yield ()
            }
          } yield ()
          //          __________________________________________

//          _ <- ctx.state.update{ s=>
//
//            //          __________________________________________
//            s.copy(events = s.events ++ List(pullEvent.copy(serialNumber = s.events.length),putEvent,getEvent,delEvent ))
//          }
          _ <- pushToCloud.start

          newHeaders = Headers(
            Header.Raw(CIString("Object-Id"), guid),
            Header.Raw(CIString("Object-Size"), objectSize.toString ),
            Header.Raw(CIString("Level"),currentLevel.toString),
            Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
            Header.Raw(CIString("Evicted-Object-Id"),evictedElement.key),
            Header.Raw(CIString("Evicted-Object-Size"),evictedElement.value.bytes.length.toString)
          )
        } yield newHeaders
        case None => for {
//          _ <- ctx.logger.debug("DOWNLOAD_NO_EVICTION")
          putServiceTime <- IO.realTime.map(_.toMillis).map( _ - arrivalTime)
          _ <- ctx.state.update{ s=>
            val putEvent = Put(
              eventId = UUID.randomUUID().toString,
              serialNumber = s.events.length+1,
              nodeId = currentNodeId,
              objectId = guid,
              objectSize = objectSize,
              timestamp = pullEvent.timestamp +10,
              milliSeconds = putServiceTime
            )
            val getEvent = Get(
              eventId = UUID.randomUUID().toString,
              serialNumber = s.events.length+2,
              nodeId = currentNodeId,
              objectId = guid,
              objectSize = objectSize,
              timestamp = pullEvent.timestamp+20,
              milliSeconds = 1L
            )
            s.copy(events = s.events :+ pullEvent.copy(serialNumber = s.events.length) :+ putEvent:+ getEvent)
          }
          newHeaders = Headers(
            Header.Raw(CIString("Object-Id"), guid),
            Header.Raw(CIString("Object-Size"),objectSize.toString ),
            Header.Raw(CIString("Level"),currentLevel.toString),
            Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
          )
        } yield newHeaders
      }
    } yield newHeaders
  }

  def putInCacheUpload(guid:String, value:ObjectS)(implicit ctx:NodeContextV6): IO[Response[IO]] = {

    for {
      arrivalTime   <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      currentEvents = currentState.events
      currentLevel  = ctx.config.level
      currentNodeId = ctx.config.nodeId
      cache         = currentState.cacheX
      levelId       = currentState.levelId
      objectSize    = value.bytes.length
      putResponse <- cache.put(key=guid,value=value)
      //        CHECK IF EVICTION OCCUR
      response    <- putResponse.evictedItem match {
        case Some(evictedElem) =>  for {
          endAt0     <- IO.realTime.map(_.toMillis)
          //            get service time
          putServiceTime      = endAt0 - arrivalTime
          //        add event
          _ <- ctx.state.update(
            s=>{
              val event = Put(
                nodeId = currentNodeId,
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length,
                objectId =guid,
                objectSize =  value.bytes.length,
                timestamp = arrivalTime,
                milliSeconds = putServiceTime
              )
              val delEvent = Del(
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length+1,
                nodeId = currentNodeId,
                objectId = evictedElem.key,
                objectSize = evictedElem.value.bytes.length,
                timestamp = arrivalTime+100,
                milliSeconds = 1
              )
              s.copy(
                events = s.events :+ event :+ delEvent
              )
            }
          )
          _          <- ctx.logger.info(s"PUT $guid $levelId $putServiceTime")
          //              Update storage of the node
          // SENT TO CLOUD
          evictedObjectExt = evictedElem.value.metadata.getOrElse("extension","bin")
          filename = s"${evictedElem.key}.$evictedObjectExt"
//          _ <- ctx.logger.debug(s"FILENAME $filename")

          pushToCloud = for{
            metadata <- Dropbox.uploadObject(currentState.dropboxClient)(
              filename = filename,
              in = new ByteArrayInputStream(evictedElem.value.bytes)
            )
//            _ <- ctx.logger.debug(s"DROPBOX_METADATA $metadata")
            //            get timestamp
            endAt     <- IO.realTime.map(_.toMillis)
            //            get service time
            time      = endAt - arrivalTime
            _ <- ctx.logger.info(s"PUT ${evictedElem.key} CLOUD $time")
            _ <- ctx.state.update{ s=>
              val pushEvent = Push(
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length,
                nodeId = currentNodeId,
                objectId = evictedElem.key,
                objectSize = evictedElem.value.bytes.length,
                pushTo = "Dropbox",
                timestamp = endAt,
                milliSeconds = time
              )
              s.copy(events = s.events :+ pushEvent)
            }
          } yield ()

          _ <- pushToCloud.start
          responsePayload         = PushResponse(
            userId= "USER_ID",
            guid=guid,
            objectSize=  value.bytes.length,
            milliSeconds = putServiceTime,
            timestamp = arrivalTime,
            level  = ctx.config.level,
            nodeId = ctx.config.nodeId
          )
          //
          response <- Ok(responsePayload.asJson,
            Headers(
              Header.Raw(CIString("Evicted-Object-Id"),evictedElem.key),
              Header.Raw(CIString("Evicted-Object-Size"),evictedElem.value.bytes.length.toString)
            )
          )
          //
        } yield response
        case None =>  for{
          endAt           <- IO.realTime.map(_.toMillis)
          putServiceTime  = endAt - arrivalTime
          responsePayload = PushResponse(
            userId= "USER_ID",
            guid=guid,
            objectSize=  objectSize,
            milliSeconds = putServiceTime,
            timestamp = endAt,
            level  = ctx.config.level,
            nodeId = ctx.config.nodeId
          )
          //        add event
          _ <- ctx.state.update(
            s=>{
              val event = Put(
                nodeId = currentNodeId,
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length,
                objectId =guid,
                objectSize =  objectSize,
                timestamp = arrivalTime,
                milliSeconds = putServiceTime
              )
              s.copy(
                events = s.events :+ event
              )
            }
          )
          response <- Ok(responsePayload.asJson)
          _ <- ctx.logger.info(s"PUT $guid $levelId $putServiceTime")
          //              else ctx.logger.info("")
        } yield response
      }
    } yield response
  }

//
//  def addNodeToLB()
//  Update storage capacity cache
//  def updateStorage(evictedItem: EvictedItemV2[ObjectS],level1ObjectSize:Long)(implicit ctx:NodeContextV6 ): IO[Unit] = {
//    for {
//      _                      <-ctx.state.update( s=>{
//        val evictedItemSize = evictedItem.value.metadata.getOrElse("objectSize","0").toLong
//        s.copy(
//          usedStorageSpace = (s.usedStorageSpace - evictedItemSize) + level1ObjectSize,
//          availableStorageSpace = (s.availableStorageSpace +evictedItemSize) - level1ObjectSize
//        )
//      }
//      )
//    } yield ()
//  }
  //
  def streamBytesToBuffer:Pipe[IO,Byte,ByteArrayOutputStream] = s0 =>
    fs2.Stream.suspend{
      s0.chunks.fold(new ByteArrayOutputStream(1000)){ (buffer,chunk)=>
        val bytes = chunk.toArraySlice
        buffer.write(bytes.values,bytes.offset,bytes.size)
        buffer
      }
    }


  def evictedItemRedirectTo(req:Request[IO],oneURL:String,evictedElem:EvictedItemV2[ObjectS],user: User)(implicit ctx:NodeContextV6): IO[Response[IO]] = for {
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
