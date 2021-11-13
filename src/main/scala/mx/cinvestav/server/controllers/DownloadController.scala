package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect.IO
import cats.effect.kernel.Outcome
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.EventXOps
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}
//
import mx.cinvestav.Declarations.{NodeContextV6, ObjectS, User}
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


  def apply()(implicit ctx:NodeContextV6) = {
      AuthedRoutes.of[User,IO]{
        case authReq@GET -> Root / "download" / UUIDVar(guid) as user => for {
          arrivalTime      <- IO.realTime.map(_.toMillis)
          arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
          currentState     <- ctx.state.get
//          _                   <- currentState.s.acquire
          currentEvents    = Events.relativeInterpretEvents(currentState.events)
          currentLevel     = ctx.config.level
          currentNodeId    = ctx.config.nodeId
          req             = authReq.req
          headers         = req.headers
          objectExt       = headers.get(CIString("Object-Extension")).map(_.head.value).getOrElse("")
          operationId     = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
//          getStartAtNanos <- IO.monotonic.map(_.toNanos)
          getStartAtNanos <- IO.monotonic.map(_.toNanos)
          maybeObject     <- Events.getObjectIds(events = currentEvents).find(_ == guid.toString).traverse(currentState.cache.lookup).map(_.flatten)
//          getEndAtNanos        <- IO.monotonic.map(_.toNanos)
          getEndAtNanos   <- IO.monotonic.map(_.toNanos)
          getStNanos      = getEndAtNanos - getStartAtNanos
          now             <- IO.realTime.map(_.toMillis)
          nowNanos        <- IO.monotonic.map(_.toNanos)
          res            <- maybeObject match {
            case Some(currentObject) => for {
              _ <- Events.saveEvents(
                events = List(
                      Get(
                        serialNumber = 0,
                        nodeId = ctx.config.nodeId,
                        objectId = guid.toString,
                        objectSize = currentObject.bytes.length,
                        timestamp = now,
                        serviceTimeNanos = getStNanos,
                        correlationId = operationId
                      )
                )
              )
              _ <- ctx.logger.info(s"GET $guid ${currentObject.bytes.length} $getStNanos $operationId")
              response <- Ok(fs2.Stream.emits(currentObject.bytes).covary[IO],
                Headers(
                  Header.Raw(CIString("Object-Id"), guid.toString),
                  Header.Raw(CIString("Object-Size"),currentObject.bytes.length.toString ),
                  Header.Raw(CIString("Level"),"LOCAL" ),
                  Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
                )
              )
            } yield response
//          MISS
            case None => for {
              _ <- IO.unit
              //         PULL FROM CLOUD
              correlationId        = operationId
//                UUID.randomUUID().toString
              filename             = s"${guid}.$objectExt"
              fileExitsInCloud     <- Dropbox.fileExists(currentState.dropboxClient)(filename = filename)
              response             <- if(!fileExitsInCloud) NotFound()
              else {
                for {
                  cloudStartAtNanos    <- IO.monotonic.map(_.toNanos)
                  out                  = new ByteArrayOutputStream()
                  retryPolicy          = RetryPolicies.limitRetries[IO](100) join RetryPolicies.exponentialBackoff[IO](2 seconds)
                  //              retryPolicy          = RetryPolicies
                  elementBytesIO       = Dropbox
                    .downloadObject(currentState.dropboxClient)(filename=filename,out=out )
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
                  newObject            = ObjectS(
                    guid=guid.toString,
                    bytes=elementBytes,
                    metadata = Map(
                      "extension" -> objectExt
                    ),

                  )
                  _                   <- currentState.cache.insert(guid.toString,newObject)
                  _maybeEvictedObject <- IO.delay(CacheX.put(events = currentEvents,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy))
                  maybeEvictedObject  <- _maybeEvictedObject.traverse(currentState.cache.lookup).map(_.flatten)
                  putEndAt            <- IO.realTime.map(_.toMillis)
                  putEndAtNanos       <- IO.monotonic.map(_.toNanos)
                  putServiceTimeNanos = putEndAtNanos - putStartAtNanos
                  //
                  evictionHeaders <- maybeEvictedObject match {
                    case Some(evictedObject) =>  for {
                      //            PUSH EVICTED OBJECT TO CLOUD
                      pushEventsFiber             <- Helpers.pushToCloud(evictedObject, currentEvents, correlationId).start
                      //                  //             DELETE EVICTED FROM CACHE
                      deleteStartAtNanos     <- IO.monotonic.map(_.toNanos)
                      _                      <- currentState.cache.delete(evictedObject.guid)
                      deleteEndAtNanos       <- IO.monotonic.map(_.toNanos)
                      deleteServiceTimeNanos = deleteEndAtNanos - deleteStartAtNanos
                      //
                      _ <- Events.saveEvents(
                        events =   List(
                          PullEvent(
                            //                        eventId = UUID.randomUUID().toString,
                            serialNumber = 0,
                            nodeId = ctx.config.nodeId,
                            objectId = newObject.guid,
                            objectSize = newObject.bytes.length,
                            pullFrom = "Dropbox",
                            timestamp = cloudEndAt,
                            serviceTimeNanos = pullServiceTimeNanos,
                            correlationId = correlationId
                          ),
                          Del(
                            //                        eventId = UUID.randomUUID().toString,
                            serialNumber = 0,
                            nodeId = currentNodeId,
                            objectId = evictedObject.guid,
                            objectSize = evictedObject.bytes.length,
                            timestamp = cloudEndAt-10,
                            serviceTimeNanos= deleteServiceTimeNanos,
                            correlationId = correlationId
                          ),
                          Put(
                            //                        eventId = UUID.randomUUID().toString,
                            serialNumber = 0,
                            nodeId = currentNodeId,
                            objectId = newObject.guid,
                            objectSize = newObject.bytes.length,
                            timestamp = putEndAt,
                            serviceTimeNanos = putServiceTimeNanos,
                            correlationId = correlationId
                          ),
                          Get(
                            //                        eventId = UUID.randomUUID().toString,
                            serialNumber = 0 ,
                            nodeId = currentNodeId,
                            objectId = newObject.guid,
                            objectSize = newObject.bytes.length,
                            timestamp = putEndAt+1,
                            serviceTimeNanos = getStNanos,
                            correlationId = correlationId
                          )
                        )
                      )
                      evictionHeaders = Headers(
                        Header.Raw(CIString("Evicted-Object-Id"),evictedObject.guid),
                        Header.Raw(CIString("Evicted-Object-Size"),evictedObject.bytes.length.toString),
                      )
                    } yield evictionHeaders
                    case None => for {
                      _            <- IO.unit
                      _ <- Events.saveEvents(
                        events = List(
                          PullEvent(
                            serialNumber = 0 ,
                            nodeId = ctx.config.nodeId,
                            objectId = newObject.guid,
                            objectSize = newObject.bytes.length,
                            pullFrom = "Dropbox",
                            timestamp = cloudEndAt,
                            serviceTimeNanos = pullServiceTimeNanos,
                            correlationId = correlationId
                          ),
                          Put(
                            serialNumber = 0,
                            nodeId = currentNodeId,
                            objectId = newObject.guid,
                            objectSize = newObject.bytes.length,
                            timestamp = putEndAt,
                            serviceTimeNanos = putServiceTimeNanos,
                            correlationId = correlationId
                          ),
                          Get(
                            serialNumber = 0,
                            nodeId = currentNodeId,
                            objectId = newObject.guid,
                            objectSize = newObject.bytes.length,
                            timestamp = putEndAt+1,
                            serviceTimeNanos = getStNanos,
                            correlationId = correlationId
                          ))
                      )
                      emptyHeaders = Headers.empty
                    } yield emptyHeaders
                    //                  IO.pure(Headers.empty)
                  }
                  //        GET
                  getServiceTimeNanos  <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
                  _              <- ctx.logger.info(s"GET $guid ${newObject.bytes.length} $getServiceTimeNanos $operationId")
                  response       <- Ok(fs2.Stream.emits(newObject.bytes).covary[IO],
                    Headers(
                      Header.Raw(CIString("Object-Id"), guid.toString),
                      Header.Raw(CIString("Object-Size"),newObject.bytes.length.toString ),
                      Header.Raw(CIString("Level"), "CLOUD"),
                      Headers(Header.Raw(CIString("Node-Id"),ctx.config.nodeId) )
                    ) ++ evictionHeaders
                  )
                } yield response
              }

//              _       <- currentState.s.release.delayBy(100 milliseconds)
            } yield response


          }
          _ <- ctx.logger.debug("____________________________________________________")
        } yield res
      }

  }

}
