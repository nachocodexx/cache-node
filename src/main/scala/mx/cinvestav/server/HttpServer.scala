package mx.cinvestav.server

import fs2.Stream
import cats.effect.std.Semaphore
import fs2.io.file.Files
import mx.cinvestav.{Declarations, Helpers}
import mx.cinvestav.Declarations.{NodeContext, ObjectD, ObjectS}
import mx.cinvestav.cache.CacheX
import mx.cinvestav.cache.CacheX.EvictedItem
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events
import mx.cinvestav.commons.events.{Del, Get, Push, Put}
import mx.cinvestav.commons.types.{ObjectLocation, ObjectMetadata}
import mx.cinvestav.events.Events
import mx.cinvestav.server.controllers.{ActiveReplication, EventsController, HitCounterController, InfoRoutes, PullController, ReplicateController, ResetController, StatsController}
import mx.cinvestav.server.middlewares.AuthMiddlewareX
import org.http4s.multipart.Part

import java.io.ByteArrayInputStream
import java.util.UUID
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
import scala.concurrent.duration._
import language.postfixOps
import org.http4s.{headers=>HEADERS}
//

class HttpServer(dSemaphore:Semaphore[IO])(implicit ctx:NodeContext){
  def apiBaseRouteName = s"/api/v${ctx.config.apiVersion}"

  def baseRoutes: Kleisli[OptionT[IO, *], Request[IO], Response[IO]] = StatsController() <+> ResetController() <+> EventsController() <+> InfoRoutes() <+> HitCounterController() <+>
    ReplicateController(dSemaphore) <+> ActiveReplication()

  private def httpApp: Kleisli[IO, Request[IO],
    Response[IO]] = Router[IO](
    s"$apiBaseRouteName" -> AuthMiddlewareX(ctx=ctx)(RouteV6(dSemaphore)),
    "/pull" -> PullController(),
    s"$apiBaseRouteName" -> baseRoutes,
    ).orNotFound
  def run()(implicit ctx:NodeContext): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
      .bindHttp(ctx.config.port,ctx.config.host)
      .withHttpApp(httpApp = httpApp)
      .withMaxConnections(ctx.config.maxConnections)
      .withResponseHeaderTimeout(ctx.config.responseHeaderTimeoutMs milliseconds)
      .withConnectorPoolSize(1000)
      .withBufferSize(ctx.config.bufferSize)
      .serve
      .compile
      .drain
  } yield ()
}
object HttpServer {
  def apply(dSemaphore:Semaphore[IO])(implicit ctx:NodeContext) = new HttpServer(dSemaphore)


}
