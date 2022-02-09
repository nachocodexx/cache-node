package mx.cinvestav.server.controllers

import breeze.linalg.sum
import cats.effect._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.events.EventXOps
//
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.Put
import mx.cinvestav.commons.types.{DumbObject, HitCounterInfo}
import mx.cinvestav.events.Events
import mx.cinvestav.events.Events.onlyPuts
//
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.io._
import org.typelevel.ci.CIString
//
import scala.concurrent.duration._
import language.postfixOps

object HitCounterController {
  def apply()(implicit ctx:NodeContext) = {
    HttpRoutes.of[IO]{
      case req@GET -> Root / "hit-counter"=> for {
        currentState                <- ctx.state.get
        headers                     = req.headers
        calculateNextNumberOfAccess = headers.get(CIString("Calculate-Next-Access")).flatMap(_.head.value.toBooleanOption).getOrElse(false)
        calculateServiceTime        = headers.get(CIString("Calculate-Service-Time")).flatMap(_.head.value.toBooleanOption).getOrElse(false)
        events                      = Events.relativeInterpretEventsMonotonic(events=currentState.events)
        objectIds                   = Events.getObjectIds(events=events)
        dumbObjects                 = Events.getDumbObjects(events=events)
        counter                     = Events.getHitCounterByNodeV2(events=events)
        mx                          = Events.generateMatrixV2(events=events)
        puts                        = onlyPuts(events = events).map(_.asInstanceOf[Put])
        userIds                     = puts.map(_.userId).distinct
        xSum                        = sum(mx)
        x                           = if(xSum == 0.0) mx.toArray.toList.map(_=>0.0) else (mx/xSum).toArray.toList
        y                           = Events.getHitCounterByUser(events=events)
        normalizeCounter            = (objectIds zip x).toMap
        objectSizes                 = dumbObjects.map{
          case o@DumbObject(objectId, objectSize) =>
            (objectId -> objectSize)
        }.toMap

        nextNumberOfAccess = if(calculateNextNumberOfAccess)
          Helpers.generateNextNumberOfAccessByObjectId(events=events)(ctx.config.intervalMs milliseconds)
        else Map.empty[String,Double]

        meanServiceTime = if(events.isEmpty) 0.0 else EventXOps.getMeanServiceTime(events = events)
        meanWaitingTime = if(events.isEmpty) 0.0 else EventXOps.getMeanWaitingTime(events = events)
        meanArrivalTime = if (events.isEmpty) 0.0 else EventXOps.getMeanArrivalTime(events = events)
        meanIdleTime    = if(events.isEmpty) 0.0 else EventXOps.getMeanIdleTime(events  = events)

        hitVec = HitCounterInfo(
          nodeId             = ctx.config.nodeId,
          objectIds          = objectIds,
          userIds            = userIds,
          hitCounter         = counter,
          normalizeCounter   = normalizeCounter,
          hitCounterByUser   = y,
          objectSizes        = objectSizes,
          nextNumberOfAccess = nextNumberOfAccess,
          meanServiceTime    = meanServiceTime,
          meanWaitingTime    = meanWaitingTime,
          meanArrivalTime    = meanArrivalTime,
          meanIdleTime       = meanIdleTime
        )
        res <- Ok(hitVec.asJson)
      } yield res
    }
  }
}
