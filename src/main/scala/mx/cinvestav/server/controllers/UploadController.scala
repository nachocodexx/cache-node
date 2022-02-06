package mx.cinvestav.server.controllers

import cats.implicits._
import fs2.Stream
import fs2.io.file.Files
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.{IObject, ObjectD}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.ObjectHashing
import org.http4s.AuthedRequest

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


  def controller(operationId:String, objectId:String)(authReq:AuthedRequest[IO,User])(implicit ctx:NodeContext)= for {
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
      currentState     <- ctx.state.get
      currentEvents    = Events.relativeInterpretEvents(currentState.events)
      req              = authReq.req
      user             = authReq.context
      multipart        <- req.as[Multipart[IO]]
      parts            = multipart.parts
      //    _______________________________________________
      responses    <- parts.traverse{ part =>
        for{
          _               <- IO.unit
          partHeaders     = part.headers
//          guid            = partHeaders.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          contentType     = partHeaders.get(HEADERS.`Content-Type`.headerInstance.name).map(_.head.value).getOrElse("application/octet-stream")
          media           = MediaType.unsafeParse(contentType)
          objectExtension = media.fileExtensions.head
          body            = part.body
          bytesBuffer     <- body.compile.to(Array)
          objectSize  = partHeaders
            .get(org.http4s.headers.`Content-Length`.name)
            .map(_.head.value)
            .getOrElse("0")

          newObject <- if(!ctx.config.inMemory) for {
            _    <- IO.unit
            meta = Map("objectSize"->objectSize, "contentType" -> contentType, "extension" -> objectExtension)
            path = Paths.get(s"${ctx.config.storagePath}/$objectId")
            o    = ObjectD(guid=objectId,path =path,metadata=meta).asInstanceOf[IObject]
            _    <- Stream.emits(bytesBuffer).covary[IO].through(Files[IO].writeAll(path)).compile.drain
          } yield o
          else ObjectS(
            guid=objectId,
            bytes= bytesBuffer,
            metadata=Map(
              "objectSize"->objectSize,
              "contentType" -> contentType,
              "extension" -> objectExtension
            )
          ).asInstanceOf[IObject].pure[IO]
          //        PUT TO CACHE
          evictedElement  <- IO.delay{CacheX.put(events = currentEvents,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy)}
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
                  evictedContentType = MediaType.unsafeParse(evictedObject.metadata.getOrElse("contentType","application/octet-stream"))
                   _ <- Helpers.pushToNextLevel(
                      evictedObjectId = evictedObjectId,
                      bytes = evictedObjectBytes,
                      metadata = evictedObject.metadata,
                      currentEvents = currentEvents,
                      correlationId = operationId,
                      delete = delete
                    ).start
                  deleteStartAtNanos     <- IO.monotonic.map(_.toNanos)
//                  _                      <-
                  _                      <- currentState.cache.delete(evictedObjectId)

//                  else currentState.cache.delete(evictedObjectId) *> Files[IO].delete()
                  deleteEndAt            <- IO.realTime.map(_.toMillis)
                  deleteEndAtNanos       <- IO.monotonic.map(_.toNanos)
                  deleteServiceTimeNanos = deleteEndAtNanos - deleteStartAtNanos
                  //                PUT NEW OBJECT IN CACHE
                  putStartAtNanos        <- IO.monotonic.map(_.toNanos)
                  _                      <- currentState.cache.insert(objectId,newObject)
                  putEndAt               <- IO.realTime.map(_.toMillis)
                  putEndAtNanos          <- IO.monotonic.map(_.toNanos)
                  putServiceTimeNanos    = putEndAtNanos - putStartAtNanos
                  delEvent = Del(
                    serialNumber = 0 ,
                    nodeId = ctx.config.nodeId,
                    objectId = evictedObjectId,
                    objectSize = evictedObjectSize,
                    timestamp =deleteEndAt,
                    serviceTimeNanos = deleteServiceTimeNanos,
                    correlationId = operationId
                  )
                  _ <- Events.saveEvents(
                    List(delEvent,
                      Put(
                        serialNumber = 0,
                        nodeId = ctx.config.nodeId,
                        objectId = newObject.guid,
                        objectSize = objectSize.toLong,
                        timestamp = putEndAt,
                        serviceTimeNanos = putServiceTimeNanos,
                        correlationId = operationId
                      )
                    ))
//                  _ <- ctx.logger.info(s"PUT $objectId $objectSize $putServiceTimeNanos $operationId")
                  newHeaders = Headers(
                    Header.Raw(CIString("Evicted-Object-Id"),evictedObjectId),
                    Header.Raw(CIString("Evicted-Object-Size"),evictedObjectSize.toString),
                    Headers(Header.Raw(CIString("Node-Id"),ctx.config.nodeId )),
                    Headers(Header.Raw(CIString("Level"),"CLOUD"  )),

                  )
                  //                      EVICTED
                  _ <- ctx.config.pool.sendEvicted(delEvent).start
                } yield newHeaders
                case None => for {
                  _ <- ctx.logger.error("WARNING: OBJECT WAS NOT PRESENT IN THE CACHE.")
                } yield Headers.empty
                //                    Headers.empty
                //                  )
              }
            } yield newHeades
            //               NO EVICTION
            case None => for {
              //             PUT NEW OBJECT
              putStartAtNanos     <- IO.monotonic.map(_.toNanos)
              _                   <- currentState.cache.insert(objectId,newObject)
              putEndAtNanos       <- IO.monotonic.map(_.toNanos)
              putServiceTimeNanos = putEndAtNanos - putStartAtNanos
              _ <- Events.saveEvents(
                events =  List(
                  Put(
                    serialNumber = 0,
                    nodeId = ctx.config.nodeId,
                    objectId = newObject.guid,
                    objectSize = objectSize.toLong,
                    timestamp = now,
                    serviceTimeNanos = putServiceTimeNanos,
                    correlationId = operationId
                  )
                )
              )
              newHeaders = Headers(
                Headers(Header.Raw(CIString("Node-Id"),ctx.config.nodeId)),
                Headers(Header.Raw(CIString("Level"), "LOCAL")),
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
            objectSize=  objectSize.toLong,
            serviceTimeNanos = putServiceTimeNanos,
            timestamp = now,
            level  = ctx.config.level,
            nodeId = ctx.config.nodeId
          ).asJson
          response <- Ok(responsePayload,newHeaders)
        } yield response
      }.map(_.head)
//      _ <- ctx.logger.debug(responses.toString)
    } yield responses
  def apply(downloadSemaphore:Semaphore[IO])(implicit ctx:NodeContext): AuthedRoutes[User, IO] = {

    AuthedRoutes.of[User,IO]{
      case authReq@POST -> Root / "upload" as user => for {

        waitingTimeStartAt <- IO.monotonic.map(_.toNanos)
        _                  <- downloadSemaphore.acquire
        operationId        = authReq.req.headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        objectId           = authReq.req.headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        waitingTimeEndAt   <- IO.monotonic.map(_.toNanos)
        waitingTime        = waitingTimeEndAt - waitingTimeStartAt
        _                  <- ctx.logger.info(s"WAITING_TIME $objectId 0 $waitingTime $operationId")
        response0           <- controller(operationId,objectId)(authReq)
        headers            = response0.headers
        objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        _                  <- downloadSemaphore.release
        serviceTimeNanos   <- IO.monotonic.map(_.toNanos).map(_ - waitingTimeStartAt)
//      ______________________________________________________________
        response           = response0.putHeaders(
          Headers(
            Header.Raw( CIString("Waiting-Time"),waitingTime.toString ),
            Header.Raw(CIString("Service-Time"),serviceTimeNanos.toString)
          )
        )
//      ____________________________________________________________
        _                  <- ctx.logger.info(s"PUT $objectId $objectSize $serviceTimeNanos $operationId")
        _                  <- ctx.logger.debug("____________________________________________________")
      } yield response
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
