package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect.IO
import cats.effect.std.Semaphore
import mx.cinvestav.commons.Implicits._
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.Get
import mx.cinvestav.commons.payloads.PutAndGet
import mx.cinvestav.events.Events
import org.http4s.HttpRoutes

import java.util.UUID
//
import mx.cinvestav.Declarations.{NodeContext, User}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.{AuthedRoutes, Header, Headers, MediaType, Method, Request, Uri}
import org.http4s.{headers=>HEADERS}
import org.http4s.dsl.io._
import org.typelevel.ci.CIString

object ReplicateController {

  def apply(semaphore: Semaphore[IO])(implicit ctx:NodeContext) = {

    HttpRoutes.of[IO]{
      case req@POST -> Root / "replicate" / objectId => for {
        serviceTimeStart  <- IO.monotonic.map(_.toNanos)
        _                 <- semaphore.acquire
        waitingTime       <- IO.monotonic.map(_.toNanos).map(_ - serviceTimeStart)
        currentState      <- ctx.state.get
//        currentEvents     = currentState.events
        operationId       = UUID.randomUUID().toString
        _                 <- ctx.logger.debug(s"REPLICATE $objectId")
        headers           = req.headers
        maybeReplicaNodes = headers.get(CIString("Nodes")).map(_.map(_.value))
        objectSize        = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        response          <- maybeReplicaNodes match {
            case Some(replicaNodes) => for {
              _                   <- ctx.logger.debug(s"FROM $replicaNodes")
//
              selectedReplicaNode =  replicaNodes.head
              uri                 =  Uri.unsafeFromString(s"http://$selectedReplicaNode:6666/api/v2/download/$objectId")
              _headers            =  Headers(
                Header.Raw(CIString("Operation-Id"), operationId ) ,
                Header.Raw(CIString("Object-Id"),objectId),
                Header.Raw(CIString("User-Id"),UUID.randomUUID().toString),
                Header.Raw(CIString("Bucket-Id"),UUID.randomUUID().toString),
                Header.Raw(CIString("Object-Size"),objectSize.toString),
              )
              request             =  Request[IO](method = Method.GET,uri =uri,headers = _headers)
//
//              (bytes,producerId)               <- ctx.client.stream(req = request)
                res             <- ctx.client.stream(req = request)
                .evalMap {
                  res =>
                    val hs = res.headers
                    for {
//                      bs         <- res.body.compile.to(Array)
                      _          <- IO.unit
                      serviceTimeStartD = hs.get(CIString("Service-Time-Start")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                      serviceTimeEndD   = hs.get(CIString("Service-Time-End")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                      serviceTimeD      = hs.get(CIString("Service-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                      waitingTimeD      = hs.get(CIString("Waiting-Time")).flatMap(_.head.value.toLongOption).getOrElse(0L)
                      producerId        = hs.get(CIString("Producer-Id")).map(_.head.value).getOrElse("PRODUCER_ID")
                      _put              <- Helpers.uploadObj(
                        operationId     = operationId,
                        objectId        = objectId,
                        objectSize      = objectSize,
                        bytesBuffer     = res.body,
                        objectExtension ="",
                        producerId      = producerId,
                        waitingTime     = waitingTime
                      ).onError(e=>ctx.logger.error(e.getMessage))
                      now               <- IO.realTime.map(_.toNanos)
                      _get              = Get(
                        serialNumber       = 0,
                        nodeId             = selectedReplicaNode,
                        objectId           = objectId,
                        objectSize         = objectSize,
                        timestamp          = now,
                        serviceTimeNanos   = serviceTimeD,
                        serviceTimeStart   = serviceTimeStartD,
                        serviceTimeEnd     = serviceTimeEndD,
                        waitingTime        = waitingTimeD,
                        userId             = ctx.config.nodeId,
                        correlationId      = operationId,
                        monotonicTimestamp = 0L
                      )
                      putAndGet         =  PutAndGet(
                        put = _put,
                        get = _get
                      )
                      _                 <- ctx.config.pool.sendPut(put= putAndGet)
                      newResponse       <- NoContent()
//                    } yield (bs,producerId)
                    } yield newResponse
              //                    val _hs = res
                }
                .onError{ e =>
                  ctx.logger.error(e.getMessage).pureS
                }
                .compile
                .lastOrError
            } yield res
            case None => NoContent()
          }
        serviceTimeEnd    <- IO.monotonic.map(_.toNanos)
        _                 <- semaphore.release
        serviceTime    = serviceTimeEnd-serviceTimeStart
        newResponse    = response.putHeaders(
          Headers(
            Header.Raw(CIString("Service-Time-Start"),serviceTimeStart.toString),
            Header.Raw(CIString("Service-Time-End"),serviceTimeEnd.toString),
            Header.Raw(CIString("Service-Time"),serviceTime.toString),
            Header.Raw(CIString("Waiting-Time"),waitingTime.toString),
          )
        )
      } yield newResponse
    }
  }

}
////        cacheX               = currentState.cacheX
//        req                  = authReq.req
//        headers              = req.headers
//        //
//        replicationStrategy  = headers.get(CIString("Replication-Strategy")).map(_.head.value).getOrElse("ACTIVE")
//        replicationFactor    = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
//        replicaNodes         = headers.get(CIString("Replica-Node")).map(x=>x.map(_.value).toList ).getOrElse(List.empty[String])
//        _                    <- ctx.logger.debug(s"REPLICATION_STRATEGY $replicationStrategy")
//        _                    <- ctx.logger.debug(s"REPLICATION_FACTOR $replicationFactor")
//        _                    <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
//        //
//        getResponse          <- cacheX.get(guid.toString)
//        newRes       <- getResponse.item match {
//          case Some(value) =>
//            if(replicationStrategy == "ACTIVE") for {
//              _              <- ctx.logger.debug("ACTIVE_REPLICATION")
//              objectX        = value.value
//              streamBytes    = fs2.Stream.emits(objectX.bytes).covary[IO]
//              objectSize     = objectX.bytes.length
//              multipart      = Multipart[IO](parts = Vector(Part[IO](
//                headers =  Headers(
//                  HEADERS.`Content-Length`(objectSize),
//                  HEADERS.`Content-Type`(MediaType.application.pdf),
//                  Header.Raw(CIString("guid"),guid.toString),
//                  Header.Raw(CIString("filename"),"default")
//                ),
//                body    =  streamBytes
//              )))
//              requests      = replicaNodes.map{ url =>
////                urls.map( url =>
//                  Request[IO](
//                    method  = Method.POST,
//                    uri     = Uri.unsafeFromString(url),
//                    headers =  multipart.headers
//                  )
//                    .withEntity(multipart)
//                    .putHeaders(
//                      Headers(
//                        Header.Raw(CIString("User-Id"),user.id.toString),
//                        Header.Raw(CIString("Bucket-Id"),user.bucketName)
//                      ),
//                    )
////                )
//              }
//              (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
//              responses          <- requests.traverse(req=>client.toHttpApp.run(req))
//              _                  <- ctx.logger.debug(responses.toString)
//              _                  <- finalizer
//              res                <- Ok("")
//            } yield res
//            else for {
//              _   <- ctx.logger.debug("PASSIVE_REPLICATION")
//              res <- Ok("")
//            } yield res
//          case None => NotFound()
//        }
//        response <- Ok("REPLICATE")
