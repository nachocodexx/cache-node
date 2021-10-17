package mx.cinvestav.server

import fs2.io.file.Files
import mx.cinvestav.{Declarations, Helpers}
import mx.cinvestav.Declarations.{NodeContextV6, ObjectS}
import mx.cinvestav.cache.CacheX.EvictedItem
import mx.cinvestav.commons.events
import mx.cinvestav.commons.events.{Del, Get, Put}
import mx.cinvestav.commons.types.ObjectLocation
//
import cats.implicits._
import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import cats.nio.file.{Files => NIOFiles}
//
import java.nio.file.Paths
//
import mx.cinvestav.Declarations.User
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.commons.events.{Pull => PullEvent}
import mx.cinvestav.events.Events
//
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.{AuthMiddleware, Router}
import org.http4s.{AuthedRoutes, HttpRoutes, Request, Response}
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.multipart.Multipart
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
//
import org.typelevel.ci._
//
import io.circe.generic.auto._
import io.circe.syntax._
//
import java.util.UUID
import scala.concurrent.ExecutionContext.global
import concurrent.duration._
import language.postfixOps
//

object HttpServer {


  def authUser()(implicit ctx:NodeContextV6):Kleisli[OptionT[IO,*],Request[IO],User] =
    Kleisli{ req=> for {
      _          <- OptionT.liftF(ctx.logger.debug("AUTH MIDDLEWARE"))
      headers    = req.headers
//      _          <- OptionT.liftF(ctx.logger.debug(headers.headers.mkString(" // ")))
      maybeUserId     = headers.get(ci"User-Id").map(_.head).map(_.value)
      maybeBucketName = headers.get(ci"Bucket-Id").map(_.head).map(_.value)
      _          <- OptionT.liftF(ctx.logger.debug(maybeUserId.toString+"//"+maybeBucketName.toString))
      ress            <- (maybeUserId,maybeBucketName) match {
        case (Some(userId),Some(bucketName)) =>   for {
          x  <- OptionT.liftF(User(id = UUID.fromString(userId),bucketName=bucketName  ).pure[IO])
//          _  <- OptionT.liftF(ctx.logger.debug("AUTHORIZED"))
        } yield x
        case (Some(_),None) => OptionT.liftF(User(id = UUID.fromString("b952f36e-e2f4-4b5a-93f1-e6d3af1b9c75"),bucketName="DEFAULT"  ).pure[IO])
        //          OptionT.none[IO,User]
        case (None,Some(_)) => OptionT.liftF(User(id = UUID.fromString("b952f36e-e2f4-4b5a-93f1-e6d3af1b9c75"),bucketName="DEFAULT"  ).pure[IO])
//          OptionT.none[IO,User]
        case (None,None )   => OptionT.liftF(User(id = UUID.fromString("b952f36e-e2f4-4b5a-93f1-e6d3af1b9c75"),bucketName="DEFAULT"  ).pure[IO])
//          OptionT.none[IO,User]
      }

      } yield ress
    }

  def authMiddleware(implicit ctx:NodeContextV6):AuthMiddleware[IO,User] =
    AuthMiddleware(authUser=authUser)


  private def httpApp()(implicit ctx:NodeContextV6): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
      "/api/v6" -> authMiddleware(ctx=ctx)(RouteV6()),
      "/pull" -> HttpRoutes.of[IO]{
        case req@POST -> Root => for {
          arrivalTime   <- IO.realTime.map(_.toMillis)
          currentState  <- ctx.state.get
          eventsCount   = currentState.events.length
          currentNodeId = ctx.config.nodeId
          headers       = req.headers
          pullFromURL   = headers.get(CIString("Pull-From")).map(_.map(_.value)).get
          _             <- ctx.logger.debug(s"PULL_FROM $pullFromURL")
          requests      = pullFromURL.map{ pullfromUrl =>
            Request[IO](method = Method.GET,uri = Uri.unsafeFromString(pullfromUrl))
          }
//
          (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
          responses    <- requests.traverse{ request =>
            for {
              startedAt   <- IO.realTime.map(_.toMillis)
              response    <- client.toHttpApp.run(request)
              serviceTime <- IO.realTime.map(_.toMillis).map(_ - startedAt)
            } yield (response,serviceTime)
          }
//
            newHeaders   <-  responses.traverse{ data => for {
            _                 <- IO.unit
            response          = data._1
            serviceTime       = data._2
            _                 <- ctx.logger.debug(s"RESPONSE $response")
            headers           = response.headers
            body              <- response.body.compile.to(Array)
            replicaObjectId   = headers.get(CIString("Object-Id")).map(_.head.value).get
            replicaObjectSize = headers.get(CIString("Object-Size")).map(_.head.value).flatMap(_.toLongOption).get
            replicaNodeId     = headers.get(CIString("Node-Id")).map(_.head.value).get
            objectX           = ObjectS(
              guid     =replicaObjectId,
              bytes    = body,
              metadata = Map(
                "objectSize"  -> replicaObjectSize.toString,
                "contentType" -> "application/octet-stream",
                "extension"   -> "bin"
              )
            )

             newEvents = (evictedItem:Option[EvictedItem[ObjectS]])=> evictedItem match {
               case Some(value) => List(
                   PullEvent(
                     eventId = UUID.randomUUID().toString,
                     serialNumber = eventsCount,
                     nodeId = currentNodeId,
                     objectId = replicaObjectId,
                     objectSize = replicaObjectSize,
                     pullFrom = replicaNodeId,
                     milliSeconds = serviceTime,
                     timestamp = arrivalTime,
                   ),
                   Del(
                     eventId = UUID.randomUUID().toString,
                     serialNumber = eventsCount+1,
                     nodeId = currentNodeId,
                     objectId = value.key,
                     objectSize = value.value.bytes.length,
                     timestamp = arrivalTime+10,
                     milliSeconds = 1L
                   ),
                   Put(
                     eventId = UUID.randomUUID().toString,
                     serialNumber =eventsCount+2,
                     nodeId = currentNodeId,
                     objectId = replicaObjectId,
                     objectSize = replicaObjectSize,
                     timestamp = arrivalTime+20,
                     milliSeconds = 1
                   )
                 )
               case None => List(
                 PullEvent(
                   eventId = UUID.randomUUID().toString,
                   serialNumber = eventsCount,
                   nodeId = currentNodeId,
                     objectId = replicaObjectId,
                     objectSize = replicaObjectSize,
                     pullFrom = replicaNodeId,
                     milliSeconds = serviceTime,
                     timestamp = arrivalTime,
                   ),
                   Put(
                     eventId = UUID.randomUUID().toString,
                     serialNumber =eventsCount+1,
                     nodeId = currentNodeId,
                     objectId = replicaObjectId,
                     objectSize = replicaObjectSize,
                     timestamp = arrivalTime+10,
                     milliSeconds = 1
                   )
                 )
             }
            _ <- Helpers.putInCacheGeneric(guid= replicaObjectId,value = objectX,newEvents = newEvents)
            putServiceTime <- IO.realTime.map(_.toMillis).map( _ - arrivalTime)
            newHeaders = Headers(
              Header.Raw(CIString("Download-Service-Time"),serviceTime.toString),
              Header.Raw(CIString("Upload-Service-Time"),putServiceTime.toString),
            )
            } yield newHeaders
              //
            }

          pullServiceTime <- IO.realTime.map(_.toMillis).map( _ - arrivalTime)
          _headers  = (newHeaders.foldLeft(Headers.empty)( _ |+| _)) |+| Headers(Header.Raw(CIString("Pull-Service-Time"),pullServiceTime.toString))
          _ <- finalizer
          response     <- Ok(
            "PULL",
            _headers
          )
        } yield response
      },
      "/api/v6/stats" -> HttpRoutes.of[IO]{
        case req@GET -> Root => for {
          currentState   <- ctx.state.get
          events         = currentState.events
          filteredEvents = EventXOps.OrderOps.byTimestamp(Events.relativeInterpretEvents(EventXOps.OrderOps.byTimestamp( events)))
          timestamp    <- IO.realTime.map(_.toMillis)
          usedCapacity  = EventXOps.calculateUsedCapacity(filteredEvents)
          totalCapacity = ctx.config.totalStorageSpace
          availableCapacity = totalCapacity-usedCapacity
//          data         = filteredEvents.
          payloadRes   = Map(
            "nodeId" -> ctx.config.nodeId.asJson,
            "ipAddress"-> currentState.ip.asJson,
            "port" -> ctx.config.port.asJson,
            "cachePolicy" -> ctx.config.cachePolicy.asJson,
            "totalStorageCapacity" -> totalCapacity.asJson,
            "usedStorageCapacity" -> usedCapacity.asJson,
            "availableStorageCapacity" -> availableCapacity.asJson,
//            "events"->filteredEvents.asJson,
            "timestamp" -> timestamp.asJson
          ).asJson
          response <- Ok(payloadRes)
        } yield response
      },
      "/api/v6/events" -> HttpRoutes.of[IO]{
        case req@GET -> Root => for {
          currentState   <- ctx.state.get
          events         = currentState.events
//          _
//          filteredEvents = Events.relativeInterpretEvents(events)
//            EventXOps.OrderOps.byTimestamp(
//          ).reverse
          response <- Ok(
//            Map(
              events.asJson
//              "filteredEvents"-> filteredEvents.asJson,
//              "rawEvents" -> EventXOps.OrderOps.byTimestamp(events).reverse.asJson
//            )
          )
        } yield response
      }
    ).orNotFound

  def run()(implicit ctx:NodeContextV6): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
    .bindHttp(ctx.config.port,ctx.config.host)
    .withHttpApp(httpApp = httpApp())
    .serve
    .compile
    .drain
  } yield ()

}
