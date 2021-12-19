package mx.cinvestav.server.controllers

import mx.cinvestav.Declarations.NodeContextV6
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import cats.effect._
import cats.implicits._

object ResetController {

  def apply()(implicit ctx:NodeContextV6): HttpRoutes[IO] = HttpRoutes.of[IO]{
    case POST@req -> Root => for {
//      currentState <- ctx.state.get
      _   <- ctx.logger.debug("RESET")
      _   <- ctx.state.update{ s=>
        s.copy(
          events  = Nil
        )
      }
      res <- NoContent()
    } yield res
  }

}
