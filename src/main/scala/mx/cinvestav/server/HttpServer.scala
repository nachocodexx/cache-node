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
import mx.cinvestav.server.controllers.{EventsController, InfoRoutes, PullController, ResetController, StatsController}
import mx.cinvestav.server.middlewares.AuthMiddlewareX

import java.io.ByteArrayInputStream
//
import cats.implicits._
import cats.data.{Kleisli, OptionT}
import cats.effect.IO
//
import mx.cinvestav.Declarations.Implicits._
//
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.Router
import org.http4s.{Request, Response}
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.multipart.Multipart
//
import org.typelevel.ci._
//
import io.circe.generic.auto._
import io.circe.syntax._
//
import scala.concurrent.ExecutionContext.global
import language.postfixOps
//

class HttpServer(dSemaphore:Semaphore[IO])(implicit ctx:NodeContextV6){
  def apiBaseRouteName = s"/api/v${ctx.config.apiVersion}"
  def baseRoutes: Kleisli[OptionT[IO, *], Request[IO], Response[IO]] = StatsController() <+> ResetController() <+> EventsController() <+> InfoRoutes()
  private def httpApp: Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
      s"$apiBaseRouteName" -> AuthMiddlewareX(ctx=ctx)(RouteV6(dSemaphore)),
      "/pull" -> PullController(),
      s"$apiBaseRouteName" -> baseRoutes
//        EventsController() <+> ResetController()
//      "/api/v6/stats" ->StatsController(),
//      "/api/v6/events" -> EventsController(),
//      "/api/v6/reset" -> ResetController()
    ).orNotFound
  def run()(implicit ctx:NodeContextV6): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
      .bindHttp(ctx.config.port,ctx.config.host)
      .withHttpApp(httpApp = httpApp)
      .serve
      .compile
      .drain
  } yield ()
}
object HttpServer {
  def apply(dSemaphore:Semaphore[IO])(implicit ctx:NodeContextV6) = new HttpServer(dSemaphore)


}
