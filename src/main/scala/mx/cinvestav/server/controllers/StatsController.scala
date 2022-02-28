package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect.IO
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.Implicits.{iObjectEncoder, objectSEncoderv2}
import mx.cinvestav.Helpers
//
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import scala.concurrent.duration._
import language.postfixOps

object StatsController {

  def apply()(implicit ctx:NodeContext) = {

    HttpRoutes.of[IO]{
      case req@GET -> Root / "stats" => for {
        currentState   <- ctx.state.get
        events         = currentState.events
        filteredEvents = Events.relativeInterpretEventsMonotonic(events=events)
//        filteredEvents = EventXOps.OrderOps.byTimestamp(Events.relativeInterpretEvents(EventXOps.OrderOps.byTimestamp( events)))
        timestamp    <- IO.realTime.map(_.toMillis)
        usedCapacity  = EventXOps.calculateUsedCapacity(filteredEvents)
        totalCapacity = ctx.config.totalStorageCapacity
        availableCapacity = totalCapacity-usedCapacity
        maybeObject     = Events.getObjectIds(events = filteredEvents)
        os <- maybeObject.traverse(o=>currentState.cache.lookup(o)).map(_.flatten)
        hitVec                      <- Helpers.getHitInfo(
          nodeId = ctx.config.nodeId,
          events = events,
          period =  ctx.config.intervalMs milliseconds
        )
//          .traverse(currentState.cache.lookup).map(_.flatten)
        //          data         = filteredEvents.
        payloadRes   = Map(
          "nodeId" -> ctx.config.nodeId.asJson,
          "ipAddress"-> currentState.ip.asJson,
          "port" -> ctx.config.port.asJson,
          "cachePolicy" -> ctx.config.cachePolicy.asJson,
          "totalStorageCapacity" -> totalCapacity.asJson,
          "usedStorageCapacity" -> usedCapacity.asJson,
          "availableStorageCapacity" -> availableCapacity.asJson,
          "timestamp" -> timestamp.asJson,
          "objects" -> os.map(x=>x.asJson(iObjectEncoder)).asJson,
          "stats" -> hitVec.asJson
        ).asJson
        response <- Ok(payloadRes)
      } yield response
    }
  }

}
