package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect.IO
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.Implicits.{iObjectEncoder, objectDEncoder}
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
        currentState      <- ctx.state.get
        nodeId            = ctx.config.nodeId
        events            = currentState.events
        filteredEvents    = Events.relativeInterpretEventsMonotonic(events=events)
//        filteredEvents = EventXOps.OrderOps.byTimestamp(Events.relativeInterpretEvents(EventXOps.OrderOps.byTimestamp( events)))
        timestamp         <- IO.realTime.map(_.toMillis)
        usedCapacity      = EventXOps.calculateUsedStorageCapacity(filteredEvents,nodeId)
        totalCapacity     = ctx.config.totalStorageCapacity
        availableCapacity = totalCapacity-usedCapacity
        maybeObject       = Events.getObjectIds(events = filteredEvents)
        os                <- maybeObject.traverse(o=>currentState.cache.lookup(o)).map(_.flatten)
//      ______________________________________________________________________________________________________________________________________________
        puts              = EventXOps.onlyPuts(events = events)
        putsATs           = puts.map(_.monotonicTimestamp)
        putsSTs           = puts.map(_.serviceTimeNanos)
        putsQueueTimes    = EventXOps.calculateQueueTimes(arrivalTimes = putsATs,serviceTimes = putsSTs)
//      ______________________________________________________________________________________________________________________________________________
        gets              = EventXOps.onlyGets(events = events)
        getsATs           = gets.map(_.monotonicTimestamp)
        getsSTs           = gets.map(_.serviceTimeNanos)
        getsQueueTimes    = EventXOps.calculateQueueTimes(arrivalTimes = getsATs,serviceTimes = getsSTs)
//      ______________________________________________________________________________________________________________________________________________
        global            = (puts ++ gets)
        globalATs         = global.map(_.monotonicTimestamp)
        globalSTs         = global.map(_.serviceTimeNanos)
        globalQueueTimes  = EventXOps.calculateQueueTimes(arrivalTimes = globalATs,serviceTimes = globalSTs)

//        hitVec            <- Helpers.getHitInfo(
//          nodeId = ctx.config.nodeId,
//          events = events,
//          period =  ctx.config.intervalMs milliseconds
//        )


//          .traverse(currentState.cache.lookup).map(_.flatten)
        //          data         = filteredEvents.
        payloadRes   = Map(
          "nodeId" -> nodeId.asJson,
          "ipAddress"-> currentState.ip.asJson,
          "port" -> ctx.config.port.asJson,
//          "cachePolicy" -> ctx.config.cachePolicy.asJson,
          "totalStorageCapacity" -> totalCapacity.asJson,
          "usedStorageCapacity" -> usedCapacity.asJson,
          "availableStorageCapacity" -> availableCapacity.asJson,
//          "usedStorageCapacityPercentage" -> (usedCapacity/totalCapacity).asJson,
//          "timestamp" -> timestamp.asJson,
          "metadata" -> currentState.metadata.map(x=>x._1-> x._2.asJson(objectDEncoder)).asJson
//            os.map(x=>x.asJson(iObjectEncoder)).asJson,
//          "putsQueueTimes" -> putsQueueTimes.asJson,
//          "getsQueueTimes" -> getsQueueTimes.asJson,
//          "globalQueueTimes" -> globalQueueTimes.asJson,
//          "hitVec" -> hitVec.asJson
        ).asJson
        response <- Ok(payloadRes)
      } yield response
    }
  }

}
