package mx.cinvestav.server

import cats.effect.std.Semaphore
import fs2.io.file.Files
import mx.cinvestav.{Declarations, Helpers}
import mx.cinvestav.Declarations.{NodeContextV6, ObjectS}
import mx.cinvestav.cache.CacheX
import mx.cinvestav.cache.CacheX.EvictedItem
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events
import mx.cinvestav.commons.events.{Del, Get, Push, Put}
import mx.cinvestav.commons.types.ObjectLocation
import mx.cinvestav.server.controllers.{EventsController, PullController, ResetController, StatsController}
import mx.cinvestav.server.middlewares.AuthMiddlewareX

import java.io.ByteArrayInputStream
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






  private def httpApp(dSemaphore:Semaphore[IO])(implicit ctx:NodeContextV6): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
      "/api/v6" -> AuthMiddlewareX(ctx=ctx)(RouteV6(dSemaphore)),
      "/pull" -> PullController(),
      "/api/v6/stats" ->StatsController(),
      "/api/v6/events" -> EventsController(),
      "/api/v6/reset" -> ResetController()
    ).orNotFound

  def run(dSemaphore:Semaphore[IO])(implicit ctx:NodeContextV6): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
    .bindHttp(ctx.config.port,ctx.config.host)
    .withHttpApp(httpApp = httpApp(dSemaphore))
    .serve
    .compile
    .drain
  } yield ()

}
