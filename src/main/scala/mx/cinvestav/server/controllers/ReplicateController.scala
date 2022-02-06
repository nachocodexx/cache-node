package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect.IO
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

  def apply()(implicit ctx:NodeContext) = {

    HttpRoutes.of[IO]{
      case req@POST -> Root / "replicate" / objectId => for {
        currentState      <- ctx.state.get
        currentEvents     = currentState.events
        operationId       = UUID.randomUUID().toString
        _                 <- ctx.logger.debug(s"REPLICATE $objectId")
        headers           = req.headers
        maybeReplicaNodes = headers.get(CIString("Nodes")).map(_.map(_.value))
        response          <- maybeReplicaNodes match {
            case Some(replicaNodes) => for {
              _                   <- ctx.logger.debug(s"FROM $replicaNodes")
              selectedReplicaNode =  replicaNodes.head
              uri                 =  Uri.unsafeFromString(s"http://$selectedReplicaNode:6666/api/v2/download/$objectId")
//
              _headers            =  Headers(
                Header.Raw(CIString("Operation-Id"), operationId ) ,
                Header.Raw(CIString("Object-Id"),objectId),
                Header.Raw(CIString("User-Id"),UUID.randomUUID().toString),
                Header.Raw(CIString("Bucket-Id"),UUID.randomUUID().toString),
                Header.Raw(CIString("Object-Size"),"0"),
              )
//
              request             =  Request[IO](method = Method.GET,uri =uri,headers = _headers)
              bytes               <- ctx.client.stream(req = request)
                .evalMap(x=>x.body.compile.to(Array))
                .onError{ e =>
                  ctx.logger.error(e.getMessage).pureS
                }
                .compile
                .lastOrError
              objectSize          =  bytes.length
              _put                <- Helpers.uploadObj(
                operationId     = operationId,
                objectId        = objectId,
                objectSize      = objectSize,
                bytesBuffer     = bytes,
                objectExtension =""
              ).onError(e=>ctx.logger.error(e.getMessage))
              now                 <- IO.realTime.map(_.toNanos)
              _get                = Get(
                serialNumber = 0,
                nodeId = selectedReplicaNode,
                objectId = objectId,
                objectSize = bytes.length,
                timestamp = now,
                serviceTimeNanos = 0L,
                userId = "SYSTEM",
                correlationId = operationId,
                monotonicTimestamp = 0L
              )
              putAndGet           =  PutAndGet(
                put = _put,
                get = _get
              )
              _                   <- ctx.config.pool.sendPut(put= putAndGet)
              res                 <- NoContent()
            } yield res
            case None => NoContent()
          }
      } yield response
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
