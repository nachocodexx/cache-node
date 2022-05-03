package mx.cinvestav.server.controllers

import cats.implicits._
import fs2.Stream
import fs2.io.file.Files
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.{IObject, ObjectD}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.{EventXOps, ObjectHashing, PutCompleted}
import mx.cinvestav.commons.types.ObjectMetadata
import org.http4s.{AuthedRequest, Response}

import java.nio.file.Paths
//
import mx.cinvestav.Declarations.{NodeContext, ObjectS, User}
import mx.cinvestav.cache.CacheX
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.{Del, Push, Put}
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.PushResponse
import mx.cinvestav.commons.security.SecureX
//
import org.http4s.{headers=>HEADERS}
import org.http4s.{AuthedRoutes, Header, Headers, MediaType}
import org.http4s.dsl.io._
import org.http4s.multipart.Multipart
import org.http4s.circe.CirceEntityEncoder._
import org.typelevel.ci.CIString
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import java.io.ByteArrayInputStream
import java.util.UUID
//
import concurrent.duration._
import language.postfixOps

object UploadController {


  def controller(operationId:String, objectId:String,objectSize:Long,objectMetadata:ObjectMetadata)(authReq:AuthedRequest[IO,User])(implicit ctx:NodeContext) = for {
    arrivalTimeNanos    <- IO.monotonic.map(_.toNanos)
    currentState        <- ctx.state.get
    events              = Events.relativeInterpretEventsMonotonic(currentState.events)
    usedStorageCap      = EventXOps.calculateUsedStorageCapacity(events=events,nodeId = ctx.config.nodeId)
    availableStorageCap = ctx.config.totalStorageCapacity - usedStorageCap
    _                   <- ctx.logger.debug(s"USED_STORAGE_CAPACITY $usedStorageCap")
    _                   <- ctx.logger.debug(s"AVAILABLE_STORAGE_CAPACITY $availableStorageCap")
    responses           <- if(availableStorageCap >= objectSize) {
      for {
        _                 <- IO.unit
        maybeCompletedPut = EventXOps.completedPutByOperationId(events = events, operationId = operationId)
        responses         <- maybeCompletedPut match {
          case Some(value) =>
            Forbidden(s"$operationId was completed", Headers(Header.Raw(CIString("Error-Msg"),s"$operationId was completed")) ).map(_.asLeft[Response[IO]])
          case None => for {
            _           <- IO.unit
            req         = authReq.req
            user        = authReq.context
            multipart   <- req.as[Multipart[IO]]
            parts       = multipart.parts
            //    _______________________________________________
            responses   <- parts.traverse{ part =>
              for{
                _               <- IO.unit
                partHeaders     = part.headers
                contentType     = partHeaders.get(HEADERS.`Content-Type`.headerInstance.name).map(_.head.value).getOrElse("application/octet-stream")
                media           = MediaType.unsafeParse(contentType)
                objectExtension = media.fileExtensions.head
                body            = part.body

                metadata = Map(
                  "objectId" -> objectId,
                  "objectSize"->objectSize.toString,
                  "extension" -> objectExtension,
                  "filePath" -> objectMetadata.filePath,
                  "compressionAlgorithm" -> objectMetadata.compressionAlgorithm,
                  "catalogId" -> objectMetadata.catalogId,
                  "digest" -> objectMetadata.digest,
                  "blockIndex" -> objectMetadata.blockIndex.toString,
                  "blockId" -> objectMetadata.blockId,
                  "contentType" -> contentType,
                  "blockTotal" -> objectMetadata.blockTotal.toString
                )
                newObject <- if(!ctx.config.inMemory) {
                  for {
                    _    <- IO.unit
                    path = Paths.get(s"${ctx.config.storagePath}/$objectId")
                    o    = ObjectD(guid=objectId,path =path,metadata=metadata).asInstanceOf[IObject]
                    _    <- body.through(Files[IO].writeAll(path)).compile.drain
                  } yield o
                }
                else {
                  for {
                    bytesBuffer  <- body.compile.to(Array)
                    o = ObjectS(
                      guid     = objectId,
                      bytes    = bytesBuffer,
                      metadata  = metadata
                    ).asInstanceOf[IObject]
                  } yield o
                }
                //        PUT TO CACHE
                evictedElement  = CacheX.put(events = events,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy)
                now             <- IO.realTime.map(_.toMillis)

                newHeaders      <- evictedElement match {
                  case Some(evictedObjectId) => for {
                    maybeEvictedObject <- currentState.cache.lookup(evictedObjectId)
                    newHeades             <- maybeEvictedObject match {
                      case Some(evictedObject) => for {
                        _                <- IO.unit
                        evictedObjectBytes <- evictedObject match {
                          case ObjectD(guid, path, metadata) => for {
                            bytes  <- Files[IO].readAll(path,chunkSize = 8192).compile.to(Array)
                          } yield bytes
                          case ObjectS(guid, bytes, metadata) => bytes.pure[IO]
                        }
                        evictedObjectSize = evictedObjectBytes.length
                        delete = evictedObject match {
                          case _:ObjectD => true
                          case _:ObjectS => false
                        }
                        _ <- Helpers.pushToNextLevel(
                          evictedObjectId = evictedObjectId,
                          bytes = evictedObjectBytes,
                          metadata = evictedObject.metadata,
                          currentEvents = events,
                          correlationId = operationId,
                          delete = delete
                        ).start
                        deleteStartAtNanos     <- IO.monotonic.map(_.toNanos)
                        _                      <- currentState.cache.delete(evictedObjectId)
                        deleteEndAt            <- IO.realTime.map(_.toMillis)
                        deleteEndAtNanos       <- IO.monotonic.map(_.toNanos)
                        deleteServiceTimeNanos = deleteEndAtNanos - deleteStartAtNanos
                        _                      <- currentState.cache.insert(objectId,newObject)

                        delEvent = Del(
                          serialNumber = 0 ,
                          nodeId = ctx.config.nodeId,
                          objectId = evictedObjectId,
                          objectSize = evictedObjectSize,
                          timestamp =deleteEndAt,
                          serviceTimeNanos = deleteServiceTimeNanos,
                          correlationId = operationId
                        )
                        _ <- Events.saveEvents(List(delEvent))
                        newHeaders = Headers(
                          Header.Raw(CIString("Evicted-Object-Id"),evictedObjectId),
                          Header.Raw(CIString("Evicted-Object-Size"),evictedObjectSize.toString),
                          Header.Raw(CIString("Node-Id"),ctx.config.nodeId ),
                          Header.Raw(CIString("Level"),"CLOUD"  ),
                          Header.Raw(CIString("Object-Size"),objectSize.toString)
                        )
                        //                      EVICTED
                        _ <- ctx.config.pool.sendEvicted(delEvent).start
                      } yield newHeaders
                      case None => for {
                        _ <- ctx.logger.error("WARNING: OBJECT WAS NOT PRESENT IN THE CACHE.")
                      } yield Headers.empty
                    }
                  } yield newHeades
                  //               NO EVICTION
                  case None => for {
                    //             PUT NEW OBJECT
                    _                   <- currentState.cache.insert(objectId,newObject)
                    newHeaders = Headers(
                      Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
                      Header.Raw(CIString("Level"), "LOCAL"),
                      Header.Raw(CIString("Object-Size"),objectSize.toString)
                    )
                    //                    Headers.empty
                  } yield newHeaders
                }
                now                 <- IO.realTime.map(_.toMillis)
                nowNanos            <- IO.monotonic.map(_.toNanos)
                //              putServiceTime      = now - arrivalTime
                putServiceTimeNanos = nowNanos - arrivalTimeNanos
                responsePayload = PushResponse(
                  userId= user.id,
                  guid=objectId,
                  objectSize=  objectSize,
                  serviceTimeNanos = putServiceTimeNanos,
                  timestamp = now,
                  level  = ctx.config.level,
                  nodeId = ctx.config.nodeId
                ).asJson
                response <- Ok(responsePayload,newHeaders)
              } yield response
            }.map(_.head)
            //          .asRight[Response[IO]]
          } yield responses.asRight[Response[IO]]
        }
      } yield responses
    }
    else {
      for {
        _   <- ctx.logger.debug(s"NO_AVAILABLE_STORAGE_CAPACITY $objectId")
        res <- Accepted().map(_.asLeft[Response[IO]])
      } yield res
    }


    } yield responses
  def apply(downloadSemaphore:Semaphore[IO])(implicit ctx:NodeContext): AuthedRoutes[User, IO] = {

    AuthedRoutes.of[User,IO]{
      case authReq@POST -> Root / "upload" as user =>
        val defaultConv = (x:FiniteDuration) => x.toNanos
        val program = for {
          serviceTimeStart     <- IO.monotonic.map(defaultConv).map(_ - ctx.initTime)
          serviceTimeStartReal <- IO.realTime.map(_.toNanos)
          //     ________________________________________________________________
          req                  = authReq.req
          headers              = req.headers
          operationId          = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectId             = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          objectSize           = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
          fileExtension        = headers.get(CIString("File-Extension")).map(_.head.value).getOrElse("")
          filePath             = headers.get(CIString("File-Path")).map(_.head.value).getOrElse(s"$objectId.$fileExtension")
          compressionAlgorithm = headers.get(CIString("Compression-Algorithm")).map(_.head.value).getOrElse("")
          requestStartAt       = headers.get(CIString("Request-Start-At")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
          catalogId            = headers.get(CIString("Catalog-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          digest               = headers.get(CIString("Digest")).map(_.head.value).getOrElse("")
          blockIndex           = headers.get(CIString("Block-Index")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
          blockTotal           = headers.get(CIString("Block-Total")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
          arrivalTime          = headers.get(CIString("Arrival-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
          collaborative        = headers.get(CIString("Collaborative")).map(_.head.value).flatMap(_.toBooleanOption).getOrElse(false)
          replicaNodes         = headers.get(CIString("Replica-Node")).map(_.map(_.value).toList).getOrElse(Nil)

          blockId              = s"${objectId}_${blockIndex}"
          objectMetadata       = ObjectMetadata(
            objectId             = objectId,
            objectSize           = objectSize,
            fileExtension        = fileExtension,
            filePath             = filePath,
            compressionAlgorithm = compressionAlgorithm,
            catalogId            = catalogId ,
            digest               = digest,
            blockIndex           = blockIndex,
            blockId              = blockId,
            blockTotal           = blockTotal
          )
//          replicaOperationId   = ""
          latency              = serviceTimeStartReal - requestStartAt
          _                    <- ctx.logger.debug(s"REAL_ARRIVAL_TIME $objectId, $serviceTimeStart")
          _                    <- ctx.logger.debug(s"SERVICE_TIME_START $objectId $serviceTimeStart")
          //      ___________________________________________________________________________________________
          maybeResponse        <- controller(operationId,objectId,objectSize,objectMetadata)(authReq)
          response             <- maybeResponse match {
          case Left(value) =>  value.pure[IO]
          case Right(value) =>
            val x = for {
             _                <- IO.unit
             //      ____________________________________________________________
             serviceTimeEnd   <- IO.monotonic.map(defaultConv).map(_ - ctx.initTime)
             _                <- ctx.logger.debug(s"SERVICE_TIME_END $objectId $serviceTimeEnd")
             //      ____________________________________________________________
             serviceTime      = serviceTimeEnd - serviceTimeStart
             _                <- ctx.logger.debug(s"SERVICE_TIME $objectId $serviceTime")
             //      ______________________________________________________________________________________
             response         = value.putHeaders(
               Headers(
                 Header.Raw(CIString("Latency"),latency.toString ),
                 Header.Raw(CIString("Service-Time"),serviceTime.toString),
                 Header.Raw(CIString("Service-Time-Start"), serviceTimeStart.toString),
                 Header.Raw(CIString("Service-Time-End"), serviceTimeEnd.toString),
               )
             )
             now                <- IO.realTime.map(defaultConv)
             put                = Put(
                 serialNumber         = 0,
                 objectId             = objectId,
                 objectSize           = objectSize,
                 timestamp            = now,
                 nodeId               = ctx.config.nodeId,
                 serviceTimeNanos     = serviceTime,
                 userId               = user.id,
                 serviceTimeEnd       = serviceTimeEnd,
                 serviceTimeStart     = serviceTimeStart,
                 correlationId        = operationId,
                 monotonicTimestamp   = 0L,
                 blockId              = blockId,
                 catalogId            = catalogId,
                 realPath             = filePath,
                 digest               = digest,
                 compressionAlgorithm = compressionAlgorithm,
                 extension            = fileExtension,
                 arrivalTime          = arrivalTime
               )
             _                  <- Events.saveEvents(events =  put :: Nil)
             _                  <- ctx.logger.info(s"PUT $operationId $objectId $objectSize $serviceTimeStart $serviceTimeEnd $serviceTime")
             _                  <- ctx.logger.debug("____________________________________________________")
             _                  <- if(collaborative) {
                    for {
                      _ <- ctx.logger.debug("REPLICA_NODES "+replicaNodes.toString())
                    } yield ()
             }
             else IO.unit

             _                  <- (ctx.config.pool.uploadCompleted(operationId, objectId,blockIndex).flatMap{ status=>
               ctx.logger.debug(s"UPLOAD_COMPLETED_STATUS $status") *> (if(status.code== 204) for{
                 timestamp <- IO.realTime.map(_.toNanos)
                 _ <- Events.saveEvents(events = PutCompleted.fromPut(put,timestamp)::Nil)
               } yield ()
               else IO.unit)
             }).start
         } yield response
        x
//            Ok()
        }
      } yield response

        program.onError{ e=>
          ctx.logger.error(e.getMessage) *> ctx.logger.debug("____________________________________________________")
        }
    }
  }

}

//                  else for {
//                    _ <- IO.unit
//                    status  <- ctx.config.cachePool.upload(
//                      objectId = evictedObjectId,
//                      bytes    = evictedObjectBytes,
//                      userId   = user.id,
//                      operationId =  operationId,
//                      contentType = evictedContentType
//                    ).onError{ t=>
//                      ctx.errorLogger.error(t.getMessage)
//                    }
//                    _ <- ctx.logger.debug(s"EVICTED_CACHE_POOL_STATUS$status")
//                  } yield ()
//                  pushEventsFiber        <-
//                DELETE EVICTED OBJECT FROM CACHE
//              beforeHashNanos <- IO.monotonic.map(_.toNanos)
//              x               <- SecureX.sha512AndHex(bytesBuffer).flatMap{ hash=>
//                for {
//                  hashServiceTime <- IO.monotonic.map(_.toNanos).map(_ - beforeHashNanos)
//                  hashNow         <- IO.realTime.map(_.toMillis)
//                  _ <- ctx.logger.info(s"HASH $guid $hash $hashServiceTime $operationId")
//                  _ <- Events.saveEvents(
//                    events = List(
//                      ObjectHashing(
//                        serialNumber = 0,
//                        nodeId = ctx.config.nodeId,
//                        objectId = guid,
//                        checksum = hash,
//                        serviceTimeNanos = hashServiceTime,
//                        timestamp = hashNow,
//                        monotonicTimestamp = 0,
//                        correlationId = operationId,
//                        algorithm = "SHA512"
//                      )
//                    )
//                  )
//                } yield ()
//              }.start
//              rawBytesLen  = bytesBuffer.length
