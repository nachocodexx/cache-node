package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.std.Semaphore
import fs2.Stream
import fs2.io.file.Files
import mx.cinvestav.Declarations.{IObject, ObjectD}
import mx.cinvestav.{Declarations, Helpers}
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.commons.payloads.PutAndGet
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}

import java.nio.file.Paths
//
import mx.cinvestav.Declarations.{NodeContext, ObjectS, User}
import mx.cinvestav.cache.CacheX
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.{Del, Get, Push, Put,Pull=>PullEvent}
import mx.cinvestav.events.Events
//
import org.http4s._
//{AuthedRoutes, Header, Headers}
import org.http4s.dsl.io._
//
import org.typelevel.ci.CIString

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.UUID
//
import concurrent.duration._
import language.postfixOps

object DownloadController {


  def controller(operationId:String)(authReq: AuthedRequest[IO,User],guid:String)(implicit ctx:NodeContext) = for {
      arrivalTime      <- IO.realTime.map(_.toMillis)
      arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
      currentState     <- ctx.state.get
      //          _                   <- currentState.s.acquire
      currentEvents    = Events.relativeInterpretEvents(currentState.events)
      currentLevel     = ctx.config.level
      currentNodeId    = ctx.config.nodeId
      req             = authReq.req
      userId          = authReq.context.id
      headers         = req.headers
      objectExt       = headers.get(CIString("Object-Extension")).map(_.head.value).getOrElse("")
      objectSize      = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      //          getStartAtNanos <- IO.monotonic.map(_.toNanos)
      getStartAtNanos <- IO.monotonic.map(_.toNanos)
      maybeObject     <- Events.getObjectIds(events = currentEvents).find(_ == guid).traverse(currentState.cache.lookup).map(_.flatten)
      //          getEndAtNanos        <- IO.monotonic.map(_.toNanos)
      getEndAtNanos   <- IO.monotonic.map(_.toNanos)
      getStNanos      = getEndAtNanos - getStartAtNanos
      now             <- IO.realTime.map(_.toMillis)
      nowNanos        <- IO.monotonic.map(_.toNanos)
      res            <- maybeObject match {
        case Some(currentObject) => for {
          bytes <- currentObject match {
            case o@ObjectD(guid, path, metadata) => Files[IO].readAll(path,chunkSize=8192).compile.to(Array)
            case o@ObjectS(guid, bytes, metadata) => bytes.pure[IO]
          }
          _ <- Events.saveEvents(
            events = List(
              Get(
                serialNumber = 0,
                nodeId = ctx.config.nodeId,
                objectId = guid,
                objectSize = bytes.length,
                timestamp = now,
                serviceTimeNanos = getStNanos,
                correlationId = operationId
              )
            )
          )
          response <- Ok(fs2.Stream.emits(bytes).covary[IO],
            Headers(
              Header.Raw(CIString("Object-Id"), guid),
              Header.Raw(CIString("Object-Size"),bytes.length.toString ),
              Header.Raw(CIString("Level"),"LOCAL" ),
              Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
              Header.Raw(CIString("Operation-Id"),operationId)
            )
          )
        } yield response
        //          MISS
        case None => for {
          _ <- ctx.logger.debug(s"MISS $guid")
          //         PULL FROM CLOUD
          correlationId        = operationId
          filename             = guid
          cloudStartAtNanos    <- IO.monotonic.map(_.toNanos)
          out                  = new ByteArrayOutputStream()
//          elementBytesIO       = Dropbox.downloadObject(currentState.dropboxClient)(filename=filename,out=out )
//          fileExitsInCloud <- Dropbox.fileExists(currentState.dropboxClient)(filename = filename)
          elementBytesIO       = if(ctx.config.cloudEnabled) Dropbox.downloadObject(currentState.dropboxClient)(filename=filename,out=out )
          else ctx.config.cachePool.download(objectId = guid,objectSize = objectSize,userId=userId,operationId =correlationId,objectExtension=objectExt)

//          fileExistsInNextLevel <- if(ctx.config.cloudEnabled)  Dropbox.fileExists(currentState.dropboxClient)(filename = filename)
//          else for {
//            _ <- IO.unit
//            _ <- Events.nextLevelUri(events = currentEvents, objectId = guid) match {
//              case Some(value) => ctx.con
//              case None => IO.unit
//            }
//          } yield (false)

          retryPolicy     = RetryPolicies.limitRetries[IO](1) join RetryPolicies.exponentialBackoff[IO](0.1 seconds)
          elementBytes    <- retryingOnAllErrors[Array[Byte]](
            policy = retryPolicy,
            onError = (e:Throwable,d:RetryDetails) => ctx.errorLogger.error(e.getMessage+s"  $guid")
          )(elementBytesIO)




          cloudEndAt           <- IO.realTime.map(_.toMillis)
          cloudEndAtNanos      <- IO.monotonic.map(_.toNanos)
          pullServiceTimeNanos = cloudEndAtNanos - cloudStartAtNanos
          _                    <- ctx.logger.info(s"PULL $guid ${elementBytes.length} $pullServiceTimeNanos $operationId")
          //        PUT
          putStartAtNanos      <- IO.monotonic.map(_.toNanos)
          meta = Map("extension" -> objectExt)
          newObject <- if(ctx.config.inMemory) ObjectS(guid=guid, bytes=elementBytes, metadata = meta).asInstanceOf[IObject].pure[IO]
          else for {
            _    <- IO.unit
            path = Paths.get(s"${ctx.config.storagePath}/$guid")
            _    <- Stream.emits(elementBytes).through(Files[IO].writeAll(path)).covary[IO].compile.drain
            o    = ObjectD(guid=guid, path = path, metadata = meta).asInstanceOf[IObject]
          } yield o
          _                   <- currentState.cache.insert(guid,newObject)
          _maybeEvictedObject <- IO.delay(CacheX.put(events = currentEvents,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy))
          maybeEvictedObject  <- _maybeEvictedObject.traverse(currentState.cache.lookup).map(_.flatten)
          putEndAt            <- IO.realTime.map(_.toMillis)
          putEndAtNanos       <- IO.monotonic.map(_.toNanos)
          putServiceTimeNanos = putEndAtNanos - putStartAtNanos
          newObjectSize = elementBytes.length
          //
          put = Put(
            serialNumber = 0,
            nodeId = currentNodeId,
            objectId = newObject.guid,
            objectSize = newObjectSize,
            timestamp = putEndAt,
            serviceTimeNanos = putServiceTimeNanos,
            correlationId = correlationId
          )
          _get = Get(
          serialNumber = 0,
          nodeId = currentNodeId,
          objectId = newObject.guid,
          objectSize = newObjectSize,
          timestamp = putEndAt+1,
          serviceTimeNanos = getStNanos,
          correlationId = correlationId
          )
          evictionHeaders <- maybeEvictedObject match {
            case Some(evictedObject) =>  for {
              //            PUSH EVICTED OBJECT TO CLOUD
              evictedBytes <- evictedObject match {
                case o@ObjectD(guid, path, metadata) => Files[IO].readAll(path,chunkSize=8192).compile.to(Array)
                case o@ObjectS(guid, bytes, metadata) => bytes.pure[IO]
              }
              evictedObjectSize =evictedBytes.length
              pushEventsFiber             <- Helpers.pushToNextLevel(evictedObject.guid,evictedBytes,evictedObject.metadata, currentEvents, correlationId).start
              //                  //             DELETE EVICTED FROM CACHE
              deleteStartAtNanos     <- IO.monotonic.map(_.toNanos)
              _                      <- evictedObject match {
                case ObjectD(guid, path, metadata) => Files[IO].delete(path) *> currentState.cache.delete(guid)
                case ObjectS(guid, bytes, metadata) => currentState.cache.delete(guid)
              }
//                  _                      <-
              deleteEndAtNanos       <- IO.monotonic.map(_.toNanos)
              deleteServiceTimeNanos = deleteEndAtNanos - deleteStartAtNanos
              //
              delEvent = Del(
                serialNumber = 0,
                nodeId = currentNodeId,
                objectId = evictedObject.guid,
                objectSize = evictedBytes.length,
                timestamp = cloudEndAt-10,
                serviceTimeNanos= deleteServiceTimeNanos,
                correlationId = correlationId
              )
              _ <- Events.saveEvents(
                events =   List(
                  PullEvent(
                    //                        eventId = UUID.randomUUID().toString,
                    serialNumber = 0,
                    nodeId = ctx.config.nodeId,
                    objectId = newObject.guid,
                    objectSize = newObjectSize,
                    pullFrom = "Dropbox",
                    timestamp = cloudEndAt,
                    serviceTimeNanos = pullServiceTimeNanos,
                    correlationId = correlationId
                  ),
                  delEvent,
                  put,
                  _get
//                  Put(
//                    //                        eventId = UUID.randomUUID().toString,
//                    serialNumber = 0,
//                    nodeId = currentNodeId,
//                    objectId = newObject.guid,
//                    objectSize = newObjectSize,
//                    timestamp = putEndAt,
//                    serviceTimeNanos = putServiceTimeNanos,
//                    correlationId = correlationId
//                  ),
//                  Get(
//                    serialNumber = 0 ,
//                    nodeId = currentNodeId,
//                    objectId = newObject.guid,
//                    objectSize = newObjectSize,
//                    timestamp = putEndAt+1,
//                    serviceTimeNanos = getStNanos,
//                    correlationId = correlationId
//                  )
                )
              )
              evictionHeaders = Headers(
                Header.Raw(CIString("Evicted-Object-Id"),evictedObject.guid),
                Header.Raw(CIString("Evicted-Object-Size"),evictedObjectSize.toString),
              )
              //                      SEND EVIVTED
              _ <- ctx.config.pool.sendEvicted(delEvent).start
            } yield evictionHeaders
            case None => for {
              _            <- IO.unit
              _ <- Events.saveEvents(
                events = List(
                  PullEvent(
                    serialNumber = 0 ,
                    nodeId = ctx.config.nodeId,
                    objectId = newObject.guid,
                    objectSize = newObjectSize,
                    pullFrom = "Dropbox",
                    timestamp = cloudEndAt,
                    serviceTimeNanos = pullServiceTimeNanos,
                    correlationId = correlationId
                  ),
                  put
                )
              )
              emptyHeaders = Headers.empty
            } yield emptyHeaders
            //                  IO.pure(Headers.empty)
          }
//          _get = Get(
//            se
//          )
          putAndGet = PutAndGet(
            put = put,
            get = _get
          )
          _ <- ctx.config.pool.sendPut(putAndGet)
          getServiceTimeNanos  <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
          _              <- ctx.logger.info(s"GET $guid ${newObjectSize} $getServiceTimeNanos $operationId")
          response       <- Ok(Stream.emits(elementBytes).covary[IO],
            Headers(
              Header.Raw(CIString("Object-Id"), guid),
              Header.Raw(CIString("Object-Size"),newObjectSize.toString ),
              Header.Raw(CIString("Level"), if(ctx.config.cloudEnabled) "CLOUD" else "CACHE"),
              Headers(Header.Raw(CIString("Node-Id"),ctx.config.nodeId) )
            ) ++ evictionHeaders
          )
//            } yield response
//          }
        } yield response


      }
    } yield res
  def apply(downloadSemaphore:Semaphore[IO])(implicit ctx:NodeContext) = {
    AuthedRoutes.of[User,IO]{
      case authReq@GET -> Root / "download" / guid as user => for {
        waitingTimeStartAt <- IO.monotonic.map(_.toNanos)
        _                  <- downloadSemaphore.acquire
        operationId        = authReq.req.headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        //        _                  <- IO.println("AFTER_OPS!")
        waitingTimeEndAt   <- IO.monotonic.map(_.toNanos)
        waitingTime        = waitingTimeEndAt - waitingTimeStartAt
        _                  <- ctx.logger.info(s"WAITING_TIME $guid 0 $waitingTime $operationId")
        response0           <- controller(operationId)(authReq,guid)
        headers            = response0.headers
        objectSize         = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        _                  <- downloadSemaphore.release
        serviceTimeNanos   <- IO.monotonic.map(_.toNanos).map(_ - waitingTimeStartAt)
        response           = response0.putHeaders(
                  Headers(
                    Header.Raw( CIString("Waiting-Time"),waitingTime.toString ),
                    Header.Raw(CIString("Service-Time"),serviceTimeNanos.toString)
                  )
        )
        _                  <- ctx.logger.info(s"GET $guid $objectSize $serviceTimeNanos $operationId")
        _ <- ctx.logger.debug("____________________________________________________")
      } yield response
    }

  }

}
