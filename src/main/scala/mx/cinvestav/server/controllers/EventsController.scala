package mx.cinvestav.server.controllers

import cats.effect.IO
//
import mx.cinvestav.Declarations.NodeContext
//
import org.http4s.HttpRoutes
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import mx.cinvestav.Declarations.Implicits._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object EventsController {

  def apply()(implicit ctx:NodeContext) = {

    HttpRoutes.of[IO]{
      case req@GET -> Root / "events" => for {
        currentState   <- ctx.state.get
        events         = currentState.events
        response <- Ok(
          events.asJson
        )
      } yield response
    }
  }

}
