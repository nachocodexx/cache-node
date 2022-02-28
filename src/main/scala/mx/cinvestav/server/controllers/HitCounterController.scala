package mx.cinvestav.server.controllers

import breeze.linalg.sum
import cats.implicits._
import cats.effect._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.commons.types.QueueTimes
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
//  implicit val hitCounterInfoEncoder = new Encoder[HitCounterInfo] {
//    override def apply(a: HitCounterInfo): Json =  Json.obj(
//        "nodeId"  -> a.nodeId.asJson,
//      "objectIds"          -> Json.Null,
//      "userIds"            -> Json.Null,
//      "hitCounter"         -> Json.Null,
//      "normalizeCounter"   -> Json.Null,
//      "hitCounterByUser"   -> Json.Null,
//      "objectSizes"        -> Json.Null,
//      "nextNumberOfAccess" -> Json.Null,
//      "uploadQueueTimes"   -> Json.Null,
//      "downloadQueueTimes" -> Json.Null,
//      "globalQueueTimes"   -> Json.Null
//
//    )
//  }
  def apply()(implicit ctx:NodeContext) = {
    HttpRoutes.of[IO]{
      case req@GET -> Root / "hit-counter"=> for {
//        currentMonotinic            <- IO.monotonic.map(_.toNanos)
        currentState                <- ctx.state.get
//        headers                     = req.headers
//        calculateNextNumberOfAccess = headers.get(CIString("Calculate-Next-Access")).flatMap(_.head.value.toBooleanOption).getOrElse(false)
//        calculateServiceTime        = headers.get(CIString("Calculate-Service-Time")).flatMap(_.head.value.toBooleanOption).getOrElse(false)
        events                      = Events.relativeInterpretEventsMonotonic(events=currentState.events)
        hitVec                      <- Helpers.getHitInfo(
          nodeId = ctx.config.nodeId,
          events = events,
          period =  ctx.config.intervalMs milliseconds
        ).handleErrorWith{ e=>
           ctx.logger.error(e.getMessage) *> ctx.logger.error(e.getStackTrace.mkString("Array(", ", ", ")")) *> HitCounterInfo.empty.pure[IO]
        }
        resJson = hitVec.asJson
        res <- Ok(resJson)
      } yield res
    }
  }
}
