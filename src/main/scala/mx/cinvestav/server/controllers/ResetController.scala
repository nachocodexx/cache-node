package mx.cinvestav.server.controllers

import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.Put

import java.nio.file.Paths
//
import fs2.Stream
import fs2.io.file.Files
//
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import cats.effect._
import cats.implicits._
import mx.cinvestav.Helpers
import mx.cinvestav.events.Events

object ResetController {

  def apply()(implicit ctx:NodeContext): HttpRoutes[IO] = HttpRoutes.of[IO]{
    case POST@req -> Root / "reset" => for {
//    ________________________________________________
      currentState <- ctx.state.get
      _            <- ctx.logger.debug("RESET")
      events       = Events.relativeInterpretEventsMonotonic(events= currentState.events)
      objectIds    = Events.onlyPuts(events=events).map(_.asInstanceOf[Put]).map(_.objectId)

      _            <- if(ctx.config.inMemory) objectIds.traverse(currentState.cache.delete).start
      else for {
        _     <- IO.unit
        sp    = ctx.config.storagePath
        paths = objectIds.map(x=>Paths.get(s"$sp/$x"))
        _     <- paths.traverse(Files[IO].delete).start
      } yield ()

      _            <- ctx.state.update{ s=>
        s.copy(
          events  = Nil,

        )
      }
      res          <- NoContent()
    } yield res
  }

}
