package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.std.Semaphore
import fs2.Stream
import fs2.io.file.Files
import mx.cinvestav.Declarations.{IObject, ObjectD}
import mx.cinvestav.{Declarations, Helpers}
import mx.cinvestav.commons.events.{EventXOps, GetCompleted}
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


  def controller(operationId:String)(authReq: AuthedRequest[IO,User], objectId:String)(implicit ctx:NodeContext): IO[Response[IO]] = for {
    arrivalTimeNanos  <- IO.monotonic.map(_.toNanos)
    currentState      <- ctx.state.get
    events            = Events.relativeInterpretEventsMonotonic(currentState.events)
    maybeCompletedGet = EventXOps.completedGetByOperationId(events = events,operationId = operationId)
    res                 <- maybeCompletedGet match {
      case Some(value) => Forbidden(s"$operationId was completed", Headers(Header.Raw(CIString("Error-Msg"),s"$operationId was completed")) )
      case None => for {
        _                <- IO.unit
        currentNodeId    = ctx.config.nodeId
        req              = authReq.req
        userId           = authReq.context.id
        headers          = req.headers
        objectExt        = headers.get(CIString("Object-Extension")).map(_.head.value).getOrElse("")
        objectSize       = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        getStartAtNanos  <- IO.monotonic.map(_.toNanos)
        maybeObject      <- Events.getObjectIds(events = events).find(_ == objectId)
          .traverse(currentState.cache.lookup)
          .map(_.flatten)
        getEndAtNanos   <- IO.monotonic.map(_.toNanos)
        getStNanos      = getEndAtNanos - getStartAtNanos
        now             <- IO.realTime.map(_.toMillis)
        res            <- maybeObject match {
          case Some(currentObject) => for {
            _     <- IO.unit
            (sBytes,objectSize) = currentObject match {
              case o@ObjectD(guid, path, metadata)  =>
                (Files[IO].readAll(path,chunkSize=8192),metadata.get("objectSize").flatMap(_.toLongOption).getOrElse(0L))
              case o@ObjectS(guid, bytes, metadata) =>
                (Stream.emits(bytes),metadata.get("objectSize").flatMap(_.toLongOption).getOrElse(0L))
            }

            response <- Ok(sBytes,
              Headers(
                Header.Raw(CIString("Object-Id"), objectId),
                Header.Raw(CIString("Object-Size"), objectSize.toString),
                Header.Raw(CIString("Level"),"LOCAL" ),
                Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
                Header.Raw(CIString("Operation-Id"),operationId),
                Header.Raw(CIString("Producer-Id"),Events.getProducerIdByObjectId(objectId,events=events).getOrElse("PRODUCER_ID"))
              )
            )
          } yield response
          //          MISS
          case None => for {
            _ <- ctx.logger.debug(s"MISS $objectId")
            //         PULL FROM CLOUD
            correlationId        = operationId
            filename             = objectId
            cloudStartAtNanos    <- IO.monotonic.map(_.toNanos)
            out                  = new ByteArrayOutputStream()
            elementBytesIO       = if(ctx.config.cloudEnabled) Dropbox.downloadObject(currentState.dropboxClient)(filename=filename,out=out )
            else ctx.config.cachePool.download(objectId = objectId,objectSize = objectSize,userId=userId,operationId =correlationId,objectExtension=objectExt)

            response <- elementBytesIO.flatMap{ elementBytes=>
              for {
                _ <- IO.unit
                cloudEndAt           <- IO.realTime.map(_.toMillis)
                cloudEndAtNanos      <- IO.monotonic.map(_.toNanos)
                pullServiceTimeNanos = cloudEndAtNanos - cloudStartAtNanos
                _                    <- ctx.logger.info(s"PULL $objectId ${elementBytes.length} $pullServiceTimeNanos $operationId")
                //        PUT
                putStartAtNanos      <- IO.monotonic.map(_.toNanos)
                meta = Map("extension" -> objectExt)
                newObject <- if(ctx.config.inMemory) ObjectS(guid=objectId, bytes=elementBytes, metadata = meta).asInstanceOf[IObject].pure[IO]
                else {
                  for {
                    _    <- IO.unit
                    path = Paths.get(s"${ctx.config.storagePath}/$objectId")
                    _    <- Stream.emits(elementBytes).through(Files[IO].writeAll(path)).covary[IO].compile.drain
                    o    = ObjectD(guid=objectId, path = path, metadata = meta).asInstanceOf[IObject]
                  } yield o
                }
                _                   <- currentState.cache.insert(objectId,newObject)
                _maybeEvictedObject <- IO.delay(CacheX.put(events = events,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy))
                maybeEvictedObject  <- _maybeEvictedObject.traverse(currentState.cache.lookup).map(_.flatten)
                putEndAt            <- IO.realTime.map(_.toMillis)
                putEndAtNanos       <- IO.monotonic.map(_.toNanos)
                putServiceTimeNanos = putEndAtNanos - putStartAtNanos
                newObjectSize       = elementBytes.length
                put                 = Put(
                  serialNumber = 0,
                  nodeId = currentNodeId,
                  objectId = newObject.guid,
                  objectSize = newObjectSize,
                  timestamp = putEndAt,
                  serviceTimeNanos = putServiceTimeNanos,
                  correlationId = correlationId
                )
                _get                = Get(
                  serialNumber = 0,
                  nodeId = currentNodeId,
                  objectId = newObject.guid,
                  objectSize = newObjectSize,
                  timestamp = putEndAt+1,
                  serviceTimeNanos = getStNanos,
                  correlationId = correlationId
                )
                evictionHeaders     <- maybeEvictedObject match {
                  case Some(evictedObject) =>  for {
                    //            PUSH EVICTED OBJECT TO CLOUD
                    evictedBytes <- evictedObject match {
                      case o@ObjectD(guid, path, metadata) => Files[IO].readAll(path,chunkSize=8192).compile.to(Array)
                      case o@ObjectS(guid, bytes, metadata) => bytes.pure[IO]
                    }
                    evictedObjectSize =evictedBytes.length
                    pushEventsFiber             <- Helpers.pushToNextLevel(evictedObject.guid,evictedBytes,evictedObject.metadata, events, correlationId).start
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
                        //                      _get
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
                }
                putAndGet           = PutAndGet(
                  put = put,
                  get = _get
                )
                _                   <- ctx.config.pool.sendPut(putAndGet)
                getServiceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - arrivalTimeNanos)
                _                   <- ctx.logger.info(s"GET $objectId ${newObjectSize} $getServiceTimeNanos $operationId")
                response            <- Ok(Stream.emits(elementBytes).covary[IO], Headers(
                  Header.Raw(CIString("Object-Id"), objectId),
                  Header.Raw(CIString("Object-Size"),newObjectSize.toString ),
                  Header.Raw(CIString("Level"), if(ctx.config.cloudEnabled) "CLOUD" else "CACHE"),
                  Header.Raw(CIString("Node-Id"),ctx.config.nodeId)
                ) ++ evictionHeaders)
              } yield response
            }
              .handleErrorWith{ e=>NotFound() }

          } yield response

        }
      } yield res
    }

    } yield res
  def apply(downloadSemaphore:Semaphore[IO])(implicit ctx:NodeContext) = {
    AuthedRoutes.of[User,IO]{
      case authReq@GET -> Root / "download" / objectId as user => for {
        _      <- IO.unit
        path   = Paths.get(s"${ctx.config.storagePath}/$objectId")
        stream = Files[IO].readAll(path = path,chunkSize = 8192)
        headers = Headers(
          Header.Raw(CIString(""),"")
        )
        res    <- Ok(stream)
      } yield res
      case authReq@GET -> Root / "downloadv2" / objectId as user => for {
        serviceTimeStart     <- IO.monotonic.map(_.toNanos)
        serviceTimeStartReal <- IO.realTime.map(_.toNanos)
        _                    <- ctx.logger.debug(s"SERVICE_TIME_START $objectId $serviceTimeStart")
        headers              = authReq.req.headers
        operationId          = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        arrivalTime          = headers.get(CIString("Arrival-Time")).map(_.head.value).flatMap(_.toLongOption).getOrElse(serviceTimeStart)
        waitingTime          <- IO.monotonic.map(_.toNanos).map(_ - serviceTimeStart)
        _                    <- ctx.logger.debug(s"WAITING_TIME $objectId $waitingTime")
        response0            <- controller(operationId)(authReq,objectId)
        headers0             = response0.headers
        requestStartAt       = headers0.get(CIString("Request-Start-At")).flatMap(_.head.value.toLongOption).getOrElse(serviceTimeStart)
        latency              = serviceTimeStartReal - requestStartAt
        serviceTimeEnd       <- IO.monotonic.map(_.toNanos)
        _                    <- ctx.logger.debug(s"SERVICE_TIME_END $objectId $serviceTimeEnd")
        serviceTimeNanos     = serviceTimeEnd - serviceTimeStart
        _                    <- ctx.logger.debug(s"SERVICE_TIME $objectId $serviceTimeNanos")
        response             = response0.putHeaders(
                  Headers(
                    Header.Raw( CIString("Latency"),latency.toString ),
                    Header.Raw( CIString("Waiting-Time"),waitingTime.toString ),
                    Header.Raw(CIString("Service-Time"),serviceTimeNanos.toString),
                    Header.Raw(CIString("Service-Time-Start"),serviceTimeStart.toString),
                    Header.Raw(CIString("Service-Time-End"),serviceTimeEnd.toString)
                  )
        )
        objectSize           = response.headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        _                    <- ctx.logger.info(s"GET $objectId $objectSize $serviceTimeStart $serviceTimeEnd $serviceTimeNanos $waitingTime $operationId")
        _    <-if(response.status.code != 200) IO.unit else for{
          _   <- IO.unit
          get = Get(
              serialNumber     = 0,
              nodeId           = ctx.config.nodeId,
              objectId         = objectId,
              objectSize       = objectSize,
              timestamp        = serviceTimeStartReal,
              serviceTimeNanos = serviceTimeNanos,
              correlationId    = operationId,
              serviceTimeEnd   = serviceTimeEnd,
              serviceTimeStart = serviceTimeStart,
              arrivalTime        = arrivalTime
            )
          _   <- Events.saveEvents(events = List(get))
          _   <- (
            ctx.config.pool.downloadCompleted(operationId  = operationId, objectId = objectId)
              .flatMap { status=>
                for {
                  timestamp <- IO.realTime.map(_.toNanos)
                  _         <- Events.saveEvents(events = GetCompleted.fromGet(get,timestamp)::Nil)
                } yield ()
              }
            ).start
        } yield ()
        _ <- ctx.logger.debug("____________________________________________________")
//        _                  <- downloadSemaphore.release

      } yield response
    }

  }

}
