package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect.IO
import mx.cinvestav.Declarations.NodeContextV6
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.Implicits.{iObjectEncoder, objectSEncoderv2}
//
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object StatsController {

  def apply()(implicit ctx:NodeContextV6) = {

    HttpRoutes.of[IO]{
      case req@GET -> Root => for {
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
          "objects" -> os.map(x=>x.asJson(iObjectEncoder)).asJson
        ).asJson
        response <- Ok(payloadRes)
      } yield response
    }
  }

}
