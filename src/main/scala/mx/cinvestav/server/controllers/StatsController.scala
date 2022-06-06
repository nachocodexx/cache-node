package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect.IO
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.Declarations.Implicits._
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
import mx.cinvestav.commons.balancer.nondeterministic

object StatsController {

  def apply()(implicit ctx:NodeContext) = {

    HttpRoutes.of[IO]{
      case req@GET -> Root / "stats" => for {
        currentState      <- ctx.state.get
        nodeId            = ctx.config.nodeId
        usedCapacity      = currentState.balls.map(_.size).sum
        totalCapacity     = ctx.config.totalStorageCapacity
        availableCapacity = totalCapacity-usedCapacity
        payloadRes   = Map(
          "nodeId" -> nodeId.asJson,
          "ipAddress"-> currentState.ip.asJson,
          "port" -> ctx.config.port.asJson,
          "totalStorageCapacity" -> totalCapacity.asJson,
          "usedStorageCapacity" -> usedCapacity.asJson,
          "availableStorageCapacity" -> availableCapacity.asJson,
          "uf" -> nondeterministic.utils.calculateUF( total = totalCapacity,used = usedCapacity,objectSize = 0L).asJson,
          "balls" -> currentState.balls.asJson,
          "completedOperations" -> currentState.completedOperations.asJson
        ).asJson
        response <- Ok(payloadRes)
      } yield response
    }
  }

}
