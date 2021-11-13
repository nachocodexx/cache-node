package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect.IO
import mx.cinvestav.Declarations.{NodeContextV6, ObjectS}
import mx.cinvestav.Helpers
import mx.cinvestav.cache.CacheX
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.{Del, Push, Put, Pull => PullEvent}
import mx.cinvestav.events.Events
import org.http4s.{Header, Headers, HttpRoutes, Method, Request, Response, Uri}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.dsl.io._
import org.typelevel.ci.CIString
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}

import java.io.ByteArrayInputStream
import java.util.UUID
import scala.concurrent.ExecutionContext.global
import concurrent.duration._
import language.postfixOps

object PullController {


  def apply()(implicit ctx:NodeContextV6) = {

    HttpRoutes.of[IO]{
      case req@POST -> Root => for {
        arrivalTime      <- IO.realTime.map(_.toMillis)
        arrivalTimeNanos <- IO.monotonic.map(_.toNanos)
        currentState     <- ctx.state.get
//        _              <- currentState.s.acquire
        currentEvents    = Events.relativeInterpretEvents(currentState.events)
        eventsCount      = currentState.events.length
        currentNodeId    = ctx.config.nodeId
        headers          = req.headers
        pullFromURL      = headers.get(CIString("Pull-From")).map(_.map(_.value)).get
        requests         = pullFromURL.map{ pullfromUrl =>
          Request[IO](method = Method.GET,uri = Uri.unsafeFromString(pullfromUrl))
        }
        // ________________________________________________-
        (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
        responses    <- requests.traverse{ request =>
          for {
            startedAtNanos   <- IO.monotonic.map(_.toNanos)
            retryPolicy      = RetryPolicies.limitRetries[IO](5) join RetryPolicies.exponentialBackoff[IO](5 seconds)
            response         <-retryingOnAllErrors[Response[IO]](
              policy = retryPolicy,
              onError = (e:Throwable,d:RetryDetails)=>ctx.errorLogger.error(e.getMessage)
            )(client.toHttpApp.run(request))
            serviceTimeNanos <- IO.monotonic.map(_.toNanos).map(_ - startedAtNanos)
          } yield (response,serviceTimeNanos)
        }
        pullEndAt            <- IO.realTime.map(_.toMillis)
        //_________________________________________________________________________-
        newHeaders   <-  responses.traverse{ data => for {
          _                 <- IO.unit
          response          = data._1
          serviceTimeNanos  = data._2
//          _                 <- ctx.logger.debug(s"RESPONSE $response")
          headers           = response.headers
          body              <- response.body.compile.to(Array)
//        REPLICA METADATA
          replicaObjectId    = headers.get(CIString("Object-Id")).map(_.head.value).get
          replicaObjectSize  = headers.get(CIString("Object-Size")).map(_.head.value).flatMap(_.toLongOption).get
          replicaContentType = headers.get(CIString("Object-Content-Type")).map(_.head.value).getOrElse("application/octet-stream")
          replicaExtension   = headers.get(CIString("Object-Extension")).map(_.head.value).getOrElse("bin")
          replicaNodeId      = headers.get(CIString("Node-Id")).map(_.head.value).get
          replicaLevel       = headers.get(CIString("Level")).map(_.head.value).get
          operationId        = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          _                 <- ctx.logger.info(s"PULL_REPLICA $replicaObjectId $replicaObjectSize $serviceTimeNanos $operationId")
//        NEW OBJECT
          newObject          = ObjectS(
            guid     =replicaObjectId,
            bytes    = body,
            metadata = Map(
              "objectSize"  -> replicaObjectSize.toString,
              "contentType" -> replicaContentType,
              "extension"   -> replicaExtension
            )
          )
//
          evictedElement <- IO.delay{CacheX.put(events = currentEvents,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy)}
          putEndAt       <- IO.realTime.map(_.toMillis)
          newHeaders     <- evictedElement match {
//          EVICTION
            case Some(evictedObjectId) => for {
              maybeEvictedObject    <- currentState.cache.lookup(evictedObjectId)
              newHeades             <- maybeEvictedObject match {
                case Some(evictedObject) => for {
                  _                <- IO.unit
                  correlationId    = operationId
//                    UUID.randomUUID().toString
                  evictedObjectExt = evictedObject.metadata.getOrElse("extension","bin")
                  filename         = s"${evictedObjectId}.$evictedObjectExt"
                  //                 PUSH EVICTED OBJECT TO CLOUD
                  pushEvent        <- Helpers.pushToCloud(evictedObject, currentEvents, correlationId).start
                  //                DELETE EVICTED OBJECT FROM CACHE
                  deleteStartAtNanos     <- IO.monotonic.map(_.toNanos)
                  _                      <- currentState.cache.delete(evictedObjectId)
                  deleteEndAtNanos       <- IO.monotonic.map(_.toNanos)
                  deleteEndAt            <- IO.realTime.map(_.toMillis)
                  deleteServiceTimeNanos = deleteEndAtNanos - deleteStartAtNanos
                  //                PUT NEW OBJECT IN CACHE
                  putStartAtNanos <- IO.monotonic.map(_.toNanos)
                  _               <- currentState.cache.insert(replicaObjectId,newObject)
                  putEndAt        <- IO.realTime.map(_.toMillis)
                  putEndAtNanos   <- IO.monotonic.map(_.toNanos)
                  putServiceTimeNanos  = putEndAtNanos - putStartAtNanos
                  _ <- Events.saveEvents(
                    events = List(
                        Del(
                         serialNumber = 0,
                          nodeId = ctx.config.nodeId,
                          objectId = evictedObjectId,
                          objectSize = evictedObject.bytes.length,
                          timestamp = arrivalTime,
                          serviceTimeNanos = deleteServiceTimeNanos,
                          correlationId = correlationId
                        ),
                        Put(
                          serialNumber = 0,
                          nodeId = ctx.config.nodeId,
                          objectId = newObject.guid,
                          objectSize =newObject.bytes.length,
                          timestamp = putEndAt,
                          serviceTimeNanos = putServiceTimeNanos,
                          correlationId = correlationId
                        )
                      )
                  )

                  _ <- ctx.logger.info(s"PUT_REPLICA $replicaObjectId $replicaObjectSize $putServiceTimeNanos $correlationId")
//                  _                 <- ctx.logger.info(s"PULL_REPLICA $replicaObjectId $replicaObjectSize $serviceTimeNanos 0 $operationId")
                  newHeaders = Headers(
                    Header.Raw(CIString("Evicted-Object-Id"),evictedObjectId),
                    Header.Raw(CIString("Evicted-Object-Size"),evictedObject.bytes.length.toString),
                    Header.Raw(CIString("Download-Service-Time"),serviceTimeNanos.toString),
                    Header.Raw(CIString("Upload-Service-Time"),putServiceTimeNanos.toString),
                    Header.Raw(CIString("Node-Id"),ctx.config.nodeId ),
                    Header.Raw(CIString("Level"), "CLOUD" ),
                  )
                } yield newHeaders
                case None => for {
                  _ <- ctx.logger.error("WARNING INCONSISTENT STATE: OBJECT WAS NOT PRESENT IN THE CACHE.")
                } yield Headers.empty
              }
            } yield newHeades
//          ________________________________________________________________________________
            case None => for {
              //             NO EVICTION
              //             PUT NEW OBJECT
              putStartAtNanos     <- IO.monotonic.map(_.toNanos)
              correlationId       = operationId
//                UUID.randomUUID().toString
              _                   <- currentState.cache.insert(replicaObjectId,newObject)
              putEndAtNanos       <- IO.monotonic.map(_.toNanos)
              putServiceTimeNanos = putEndAtNanos - putStartAtNanos
              _ <- Events.saveEvents(
                events = List(
                    PullEvent(
                      serialNumber = 0,
                      nodeId = currentNodeId,
                      objectId = replicaObjectId,
                      objectSize = replicaObjectSize,
                      pullFrom = replicaNodeId,
                      serviceTimeNanos = serviceTimeNanos,
                      timestamp = pullEndAt,
                      correlationId = correlationId
                    ),
                    Put(
                      serialNumber = 0,
                      nodeId = currentNodeId,
                      objectId = replicaObjectId,
                      objectSize =replicaObjectSize,
                      timestamp = putEndAt,
                      serviceTimeNanos = putServiceTimeNanos,
                      correlationId = correlationId
                    )
                  )
              )
              _ <- ctx.logger.info(s"PUT_REPLICA $replicaObjectId $replicaObjectSize $putServiceTimeNanos $operationId")
              //              _ <- currentState.cache.insert(guid,newObject)
              newHeaders = Headers(
                Header.Raw(CIString("Download-Service-Time"),serviceTimeNanos.toString),
                Header.Raw(CIString("Upload-Service-Time"),putServiceTimeNanos.toString),
                Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
                Header.Raw(CIString("Level"), "LOCAL"),
              )
            } yield newHeaders
          }
        } yield newHeaders
          //
        }

        pullServiceTimeNano <- IO.monotonic.map(_.toNanos).map( _ - arrivalTimeNanos)
        _headers        = newHeaders.foldLeft(Headers.empty)( _ |+| _) |+| Headers(Header.Raw(CIString("Pull-Service-Time"),pullServiceTimeNano.toString))
        _               <- finalizer
        _ <- ctx.logger.debug("____________________________________________________")
        response     <- Ok(
          "PULL",
          _headers
        )
//        _              <- currentState.s.release.delayBy(100 milliseconds)
      } yield response
    }
  }

}
