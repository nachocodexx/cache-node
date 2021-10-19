package mx.cinvestav.server
// Cats
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.{NodeContextV6, objectSEncoder}
import mx.cinvestav.cache.CacheX
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.{Del, Push}
import org.http4s.{MediaType, Method}
import org.http4s.blaze.client.BlazeClientBuilder

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.concurrent.ExecutionContext.global
// Cats NIO
import cats.nio.file.{Files=>NIOFiles}
// Fs2
import fs2.io.file.Files
// Local
import mx.cinvestav.Declarations.{NodeContextV5, ObjectS, User}
import mx.cinvestav.events.Events
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.{Put,Get,Pull=>PullEvent,EventXOps}
// Http4s
import org.http4s.{Header, Headers, Request, Uri}
import org.http4s.{headers=>HEADERS}
import org.http4s.multipart.{Multipart, Part}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.AuthedRoutes
import org.typelevel.ci.CIString
import org.http4s.implicits._
import org.http4s.dsl.io._
// Circe
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
// Native
import java.nio.file.Paths
import java.util.UUID

object RouteV6 {

  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           milliSeconds:Long,
                           timestamp:Long,
                           level:Int
//                           evictedItem
                         )

  def apply()(implicit ctx:NodeContextV6): AuthedRoutes[User, IO] =  AuthedRoutes.of[User,IO]{
    case authReq@POST -> Root / "upload" as user => for {
      arrivalTime  <- IO.realTime.map(_.toMillis)
      currentState <- ctx.state.get
      currentEvents = currentState.events
//      currentLevel = ctx.config.level
//      currentNodeId = ctx.config.nodeId
//      cache        = currentState.cacheX
      req          = authReq.req
      multipart    <- req.as[Multipart[IO]]
      parts        = multipart.parts
//      levelId      = currentState.levelId
//    _______________________________________________
      responses    <- parts.traverse{ part =>
        for{
//          _           <- ctx.logger.debug(part.headers.toString)
          _           <- IO.unit
          partHeaders = part.headers
          guid        = partHeaders.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          contentType = partHeaders.get(HEADERS.`Content-Type`.headerInstance.name).map(_.head.value).getOrElse("application/octet-stream")
          media       = MediaType.unsafeParse(contentType)
          objectExtension = media.fileExtensions.head
          body        = part.body
          bytesBuffer <- body.compile.to(Array)
          //            .through(Helpers.streamBytesToBuffer)
          rawBytesLen  = bytesBuffer.length
//          _ <- ctx.logger.debug(s"RAW_BYTES $rawBytesLen")
          objectSize  = partHeaders
            .get(org.http4s.headers.`Content-Length`.name)
            .map(_.head.value)
            .getOrElse("0")

          newObject       = ObjectS(
            guid=guid,
            bytes= bytesBuffer,
            metadata=Map(
              "objectSize"->objectSize,
              "contentType" -> contentType,
              "extension" -> objectExtension
            )
          )
          //        PUT TO CACHE
          evictedElement  = CacheX.put(events = currentEvents,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy)
          now         <- IO.realTime.map(_.toMillis)
//          serviceTime = now - arrivalTime
          newHeaders <- evictedElement match {
            case Some(evictedObjectId) => for {
//              _                  <- IO.println(s"EVICTED_OBJECT $evictedObjectId")
              maybeEvictedObject <- currentState.cache.lookup(evictedObjectId)
              newHeades             <- maybeEvictedObject match {
                case Some(evictedObject) => for {
                  _                <- IO.unit
                  evictedObjectExt = evictedObject.metadata.getOrElse("extension","bin")
                  filename         = s"${evictedObjectId}.$evictedObjectExt"
//                 PUSH EVICTED OBJECT TO CLOUD
                  alreadyPushed = Events.alreadyPushedToCloud(objectId = evictedObject.guid,events = currentEvents)
                  pushEvent             <- if(!alreadyPushed) for {

                    beforePush <- IO.realTime.map(_.toMillis)
                    metadata <- Dropbox.uploadObject(currentState.dropboxClient)(
                      filename = filename,
                      in = new ByteArrayInputStream(evictedObject.bytes)
                    )
                    serviceTimePush <- IO.realTime.map(_.toMillis).map( _ - beforePush)
                    pushEvent = Push(eventId = UUID.randomUUID().toString,
                      serialNumber = currentEvents.length,
                      nodeId = ctx.config.nodeId,
                      objectId = evictedObjectId,
                      objectSize = evictedObject.bytes.length,
                      pushTo = "Dropbox",
                      timestamp = now,
                      milliSeconds = serviceTimePush
                    )
                    _ <- ctx.logger.info(s"PUSH ${evictedObject.guid} $serviceTimePush")
                  } yield List(pushEvent)
                  else IO.pure(List.empty[Push]) <* ctx.logger.debug(s"ALREADY_PUSHED ${evictedObject.guid}")
//                DELETE EVICTED OBJECT FROM CACHE
                  deleteStartAt <- IO.realTime.map(_.toMillis)
                  _ <- currentState.cache.delete(evictedObjectId)
                  deleteEndAt <- IO.realTime.map(_.toMillis)
                  deleteServiceTime = deleteEndAt - deleteStartAt
//                PUT NEW OBJECT IN CACHE
                  putStartAt <- IO.realTime.map(_.toMillis)
                  _ <- currentState.cache.insert(guid,newObject)
                  putEndAt <- IO.realTime.map(_.toMillis)
                  putServiceTime = putEndAt - putStartAt
//                  _ <- ctx.logger.debug(s"EVICTED_OBJECT_FILENAME $filename")
                  _ <- ctx.state.update{ s=>
                    val newEvents = pushEvent ++ List(
                      Del(eventId = UUID.randomUUID().toString,
                        serialNumber = s.events.length+1,
                        nodeId = ctx.config.nodeId,
                        objectId = evictedObjectId,
                        objectSize = evictedObject.bytes.length,
                        timestamp = now + 1,
                        milliSeconds = deleteServiceTime
                      ),
                      Put(
                        eventId = UUID.randomUUID().toString,
                        serialNumber = s.events.length+2,
                        nodeId = ctx.config.nodeId,
                        objectId = newObject.guid,
                        objectSize =newObject.bytes.length,
                        timestamp = putEndAt,
                        milliSeconds = putServiceTime
                      )
                    )
                    s.copy(events =  s.events ++ newEvents)
                  }
                  _ <- ctx.logger.info(s"PUT $guid $putServiceTime")
                  newHeaders = Headers(
                      Header.Raw(CIString("Evicted-Object-Id"),evictedObjectId),
                      Header.Raw(CIString("Evicted-Object-Size"),evictedObject.bytes.length.toString)
                    )
                } yield newHeaders
                case None => for {
                  _ <- ctx.logger.error("WARNING INCONSISTENT STATE: OBJECT WAS NOT PRESENT IN THE CACHE.")
                } yield Headers.empty
//                    Headers.empty
//                  )
              }
            } yield newHeades
            case None => for {
//             NO EVICTION
//             PUT NEW OBJECT
              putStartAt <- IO.realTime.map(_.toMillis)
              _ <- currentState.cache.insert(guid,newObject)
              putEndAt <- IO.realTime.map(_.toMillis)
              putServiceTime = putEndAt - putStartAt
              _ <- ctx.state.update{ s=>
                val newEvents = List(
                  Put(
                    eventId = UUID.randomUUID().toString,
                    serialNumber = s.events.length,
                    nodeId = ctx.config.nodeId,
                    objectId = newObject.guid,
                    objectSize =newObject.bytes.length,
                    timestamp = now,
                    milliSeconds = putServiceTime
                  )
                )
                s.copy(events =  s.events ++ newEvents)
              }
              _ <- ctx.logger.info(s"PUT $guid $putServiceTime")
//              _ <- currentState.cache.insert(guid,newObject)
              newHeaders = Headers.empty
            } yield newHeaders
          }
          now <- IO.realTime.map(_.toMillis)
          putServiceTime = now - arrivalTime
          responsePayload = PushResponse(
            userId= "USER_ID",
            guid=guid,
            objectSize=  newObject.bytes.length,
            milliSeconds = putServiceTime,
            timestamp = now,
            level  = ctx.config.level,
            nodeId = ctx.config.nodeId
          ).asJson
          response <- Ok(responsePayload,newHeaders)
//          response <- Helpers.putInCacheUpload(guid= guid, value = value)
        } yield response
      }.map(_.head)
      _ <- ctx.logger.debug(responses.toString)
      _ <- ctx.logger.debug("____________________________________________________")
    } yield responses
    case authReq@POST -> Root / "replicate" / UUIDVar(guid) as user => for {
      currentState <- ctx.state.get
      cacheX               = currentState.cacheX
      req                  = authReq.req
      headers              = req.headers
//
      replicationStrategy  = headers.get(CIString("Replication-Strategy")).map(_.head.value).getOrElse("ACTIVE")
      replicationFactor    = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
      replicaNodes         = headers.get(CIString("Replica-Node")).map(_.map(_.value))
//
      _                    <- ctx.logger.debug(s"REPLICATION_STRATEGY $replicationStrategy")
      _                    <- ctx.logger.debug(s"REPLICATION_FACTOR $replicationFactor")
      _                    <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
//
      getResponse          <- cacheX.get(guid.toString)
      newRes       <- getResponse.item match {
        case Some(value) =>
          if(replicationStrategy == "ACTIVE") for {
            _              <- ctx.logger.debug("ACTIVE_REPLICATION")
            objectX        = value.value
            streamBytes    = fs2.Stream.emits(objectX.bytes).covary[IO]
            objectSize     = objectX.bytes.length
            multipart      = Multipart[IO](parts = Vector(Part[IO](
              headers =  Headers(
                HEADERS.`Content-Length`(objectSize),
                HEADERS.`Content-Type`(MediaType.application.pdf),
                Header.Raw(CIString("guid"),guid.toString),
                Header.Raw(CIString("filename"),"default")
              ),
              body    =  streamBytes
            )))
            requests      = replicaNodes.map{ urls =>
              urls.map( url =>
                Request[IO](
                  method  = Method.POST,
                  uri     = Uri.unsafeFromString(url),
                  headers =  multipart.headers
                )
                  .withEntity(multipart)
                  .putHeaders(
                    Headers(
                      Header.Raw(CIString("User-Id"),user.id.toString),
                      Header.Raw(CIString("Bucket-Id"),user.bucketName)
                    ),
                  )
              )
            }
            (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
            responses          <- requests.traverse(_.traverse(req=>client.toHttpApp.run(req)))
            _                  <- ctx.logger.debug(responses.toString)
            _                  <- finalizer
            res                <- Ok("")
          } yield res
          else for {
            _   <- ctx.logger.debug("PASSIVE_REPLICATION")
            res <- Ok("")
          } yield res
        case None => NotFound()
      }
      response <- Ok("REPLICATE")
    } yield response

    case authReq@GET -> Root / "download" / UUIDVar(guid) as user => for {
      arrivalTime   <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      currentEvents = currentState.events
      currentLevel  = ctx.config.level
      currentNodeId = ctx.config.nodeId
      cache         = currentState.cacheX
      req           = authReq.req
      headers       = req.headers
      objectExt     = headers.get(CIString("Object-Extension")).map(_.head.value).getOrElse("")
      levelId       = currentState.levelId
      getStartAt    <- IO.realTime.map(_.toMillis)
      maybeObject   <- Events.getObjectIds(events = currentEvents).find(_ == guid.toString).traverse(currentState.cache.lookup).map(_.flatten)
      getEndAt      <- IO.realTime.map(_.toMillis)
      getSt         = getEndAt - getStartAt
      now           <- IO.realTime.map(_.toMillis)
      res           <- maybeObject match {
        case Some(currentObject) => for {
          _ <- ctx.state.update{ s=>
            val newEvent = Get(
              eventId = UUID.randomUUID().toString,
              serialNumber = s.events.length,
              nodeId = ctx.config.nodeId,
              objectId = guid.toString,
              objectSize = currentObject.bytes.length,
              timestamp = now,
              milliSeconds = 1
            )
            s.copy(events = s.events :+ newEvent)
          }

          _ <- ctx.logger.info(s"GET $guid $getSt")
          response <- Ok(fs2.Stream.emits(currentObject.bytes).covary[IO],
            Headers(
              Header.Raw(CIString("Object-Id"), guid.toString),
              Header.Raw(CIString("Object-Size"),currentObject.bytes.length.toString ),
              Header.Raw(CIString("Level"),currentLevel.toString),
              Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
            )
          )
        } yield response
        case None => for {
//          _ <- ctx.logger.debug("MISS")
          _ <- IO.unit
          //         PULL FROM CLOUD
          filename        = s"${guid}.$objectExt"
          cloudStartAt    <- IO.realTime.map(_.toMillis)
          out             = new ByteArrayOutputStream()
          elementBytes    <- Dropbox.downloadObject(currentState.dropboxClient)(filename=filename,out=out )
          cloudEndAt      <- IO.realTime.map(_.toMillis)
          pullServiceTime = cloudEndAt - cloudStartAt
          _               <- ctx.logger.info(s"PULL $guid $pullServiceTime")
          //        PUT
          putStartAt      <- IO.realTime.map(_.toMillis)
          newObject       = ObjectS(
            guid=guid.toString,
            bytes=elementBytes,
            metadata = Map(
              "extension" -> objectExt
            ),

          )
          _               <- currentState.cache.insert(guid.toString,newObject)
          _maybeEvictedObject = CacheX.put(events = currentEvents,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy)

          maybeEvictedObject <- _maybeEvictedObject.traverse(currentState.cache.lookup).map(_.flatten)

//          _ <- ctx.logger.debug(s"_MAYBE_EVICTED_OBJECT ${_maybeEvictedObject}")
//          _ <- ctx.logger.debug(s"MAYBE_EVICTED_OBJECT $maybeEvictedObject")

          putEndAt        <- IO.realTime.map(_.toMillis)
          putServiceTime  = putEndAt - putStartAt
          //
          _               <- ctx.state.update{ s=>
            val newEvents = List(
              PullEvent(
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length,
                nodeId = ctx.config.nodeId,
                objectId = guid.toString,
                objectSize = elementBytes.length,
                pullFrom = "Dropbox",
                timestamp = cloudEndAt,
                milliSeconds = pullServiceTime
              ),
              Put(
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length+1,
                nodeId = currentNodeId,
                objectId = newObject.guid,
                objectSize = newObject.bytes.length,
                timestamp = putEndAt,
                milliSeconds = putServiceTime
              ),
              Get(
                eventId = UUID.randomUUID().toString,
                serialNumber = s.events.length+2,
                nodeId = currentNodeId,
                objectId = newObject.guid,
                objectSize = newObject.bytes.length,
                timestamp = putEndAt+1,
                milliSeconds = 1
              )
            )
            s.copy(events = s.events ++ newEvents)
          }
          //
          evictionHeaders <- maybeEvictedObject match {
            case Some(evictedObject) =>  for {
//            PUSH EVICTED OBJECT TO CLOUD
              _ <- IO.unit
              alreadyPushed = Events.alreadyPushedToCloud(objectId = evictedObject.guid,events = currentEvents)
              pushEvent             <- if(!alreadyPushed) for {

                pushStartAt <- IO.realTime.map(_.toMillis)
                metadata <- Dropbox.uploadObject(currentState.dropboxClient)(
                  filename = s"${evictedObject.guid}.${evictedObject.metadata.getOrElse("extension","bin")}",
                  in = new ByteArrayInputStream(evictedObject.bytes)
                )
                pushEndAt <- IO.realTime.map(_.toMillis)
                serviceTimePush =  pushEndAt - pushStartAt
                _ <- ctx.logger.info(s"PUSH ${evictedObject.guid} $serviceTimePush")
                pushEvent = Push(eventId = UUID.randomUUID().toString,
                  serialNumber = currentEvents.length,
                  nodeId = ctx.config.nodeId,
                  objectId = evictedObject.guid,
                  objectSize = evictedObject.bytes.length,
                  pushTo = "Dropbox",
                  timestamp = pushEndAt,
                  milliSeconds = serviceTimePush
                )
              } yield List(pushEvent)
              else IO.pure(List.empty[Push]) <* ctx.logger.debug(s"ALREADY_PUSHED ${evictedObject.guid}")
              //              pushToCloudStartAt <- IO.realTime.map(_.toMillis)
              //
//              metadata   <- Dropbox.uploadObject(currentState.dropboxClient)(
//                filename = s"}",
//                in = new ByteArrayInputStream(evictedObject.bytes)
//              )
//              pushToCloudEndAt <- IO.realTime.map(_.toMillis)
//              serviceTimePush  =  pushToCloudEndAt - pushToCloudStartAt
              //             DELETE EVICTED FROM CACHE
              deleteStartAt   <- IO.realTime.map(_.toMillis)
              _               <- currentState.cache.delete(evictedObject.guid)
              deleteEndAt   <- IO.realTime.map(_.toMillis)
              deleteServiceTime = deleteEndAt - deleteStartAt
              //
              _               <- ctx.state.update{ s=>
                val newEvents = pushEvent++ List(
                  Del(
                    eventId = UUID.randomUUID().toString,
                    serialNumber = s.events.length+1,
                    nodeId = currentNodeId,
                    objectId = evictedObject.guid,
                    objectSize = evictedObject.bytes.length,
                    timestamp = deleteEndAt,
                    milliSeconds = deleteServiceTime
                  )
                )
                s.copy(events = s.events ++ newEvents)
              }
              evictionHeaders = Headers(
                Header.Raw(CIString("Evicted-Object-Id"),evictedObject.guid),
                Header.Raw(CIString("Evicted-Object-Size"),evictedObject.bytes.length.toString),
              )
            } yield evictionHeaders
            case None => IO.pure(Headers.empty)
          }
//          _ <- ctx.logger.debug(s"EVICTION_HEADERS $evictionHeaders")
          //        GET
          response <- Ok(fs2.Stream.emits(newObject.bytes).covary[IO],
            Headers(
              Header.Raw(CIString("Object-Id"), guid.toString),
              Header.Raw(CIString("Object-Size"),newObject.bytes.length.toString ),
              Header.Raw(CIString("Level"),currentLevel.toString),
              Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
            ) ++ evictionHeaders
          )
        } yield response
      }
//      response      <- Ok("RESPONSE")
//      getResponse   <- cache.get(guid.toString)

//      response      <- getResponse.item match {
//        case Some(element) => for {
//          _            <- ctx.logger.info(s"GET $guid $levelId ${element.counter}")
//          rawBytes = element.value.bytes
//          streamBytes  = fs2.Stream.emits(rawBytes).covary[IO]
//          bytesLen     =  rawBytes.length
//          _            <- ctx.logger.debug(s"RAW_BYTES $bytesLen")
//          getServiceTime <- IO.realTime.map(_.toMillis).map(_ - arrivalTime)
//          _            <- ctx.state.update{ s=>
//            val event = Get(
//              nodeId       = currentNodeId,
//              eventId      = UUID.randomUUID().toString,
//              serialNumber = s.events.length,
//              objectId     = guid.toString,
//              objectSize   = bytesLen,
//              timestamp    = arrivalTime,
//              milliSeconds = getServiceTime
//            )
//            s.copy(events = s.events :+ event)
//          }
//          response <- Ok(
//            streamBytes,
//            Headers(
//              Header.Raw(CIString("Object-Id"), element.value.guid),
//              Header.Raw(CIString("Object-Size"), bytesLen.toString ),
//              Header.Raw(CIString("Level"),currentLevel.toString),
//              Header.Raw(CIString("Node-Id"),ctx.config.nodeId)
//            ),
//          )
//
//        } yield response
//        case None => for {
////          _            <- ctx.logger.info(s"MISS $guid")
//          _            <- IO.unit
//          filename     = s"${guid}.$objectExt"
//          //          _ <- ctx.logger.debug(s"FILENAME $filename")
//          //       Download cloud
//          cloudStartAt  <- IO.realTime.map(_.toMillis)
//          out           = new ByteArrayOutputStream()
//          elementBytes  <- Dropbox.downloadObject(currentState.dropboxClient)(filename=filename,out=out )
//          cloudEndAt    <- IO.realTime.map(_.toMillis)
//          pullServiceTime = cloudEndAt - cloudStartAt
//          _            <- ctx.logger.info(s"PULL $guid $pullServiceTime")
//          //
//          newObject    = ObjectS(
//            guid= guid.toString,
//            bytes = elementBytes,
//            metadata = Map(
//              "objectSize" ->elementBytes.length.toString,
//              "contentType" -> "application/octet-stream",
//              "extension"  -> objectExt
//            )
//          )
//          //
//          pullEvent = PullEvent(
//            eventId = UUID.randomUUID().toString,
//            serialNumber = 0,
//            nodeId = currentNodeId,
//            objectId = guid.toString,
//            objectSize = elementBytes.length,
//            pullFrom = "Dropbox",
//            milliSeconds = pullServiceTime,
//            timestamp = arrivalTime,
//          )
//
//          newHeaders <- Helpers.putInCacheDownload(arrivalTime = arrivalTime,guid= guid.toString,value=newObject,pullEvent= pullEvent)
////          _ <- ctx.logger.debug(s"OUT_LEN ${out.toByteArray.length}")
////          _ <- ctx.logger.debug(s"ELEMENT_BYTES ${elementBytes.length}")
//          streamBytes  = fs2.Stream.emits(elementBytes).covary[IO]
//          response <- Ok(streamBytes, newHeaders)
//        } yield response
//      }

//      _ <- ctx.logger.debug("RESPONSE -> "+response.toString)
      _ <- ctx.logger.debug("____________________________________________________")
    } yield res

    case authReq@POST -> Root / "flush_all"  as user=> for {
      currentState <- ctx.state.get
      nodeId       = ctx.config.nodeId
      storagePath  = ctx.config.storagePath
      cache        = currentState.cacheX
      userId       = user.id
      bucketId     = user.bucketName
      elements     <- cache.getAll
      basePath     = Paths.get(s"$storagePath/$nodeId/$userId/$bucketId")
      sinkPath     =(guid:String)=>  basePath.resolve(guid)
      _            <- NIOFiles[IO].createDirectories(basePath)
      _            <- elements.traverse{ element =>
        fs2.Stream.emits(element.item.get.value.bytes).covary[IO].through(Files[IO].writeAll(sinkPath(element.key))).compile.drain
      }
      response     <- Ok("OK!")
    } yield response

  }

}
