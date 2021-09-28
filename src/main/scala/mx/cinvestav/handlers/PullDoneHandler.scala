package mx.cinvestav.handlers

import cats.data.EitherT
import cats.effect._
import cats.nio.file.{Files => NIOFIles}
import dev.profunktor.fs2rabbit.model.AmqpEnvelope
import io.circe.generic.auto._
import mx.cinvestav.Declarations._
import mx.cinvestav.cache.cache.CachePolicy
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.server.Client
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import org.typelevel.log4cats.Logger

import java.nio.file.{Path, Paths}

object PullDoneHandler {
  def apply()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val connection = rabbitMQContext.connection
      val client     = rabbitMQContext.client
      def successCallback(payload: Payloads.PullDone) = {
        val app = for {
          timestamp       <- liftFF[Long](IO.realTime.map(_.toMillis))
          currentState    <- maybeCurrentState
//          replyTo         <- maybeReplyTo
          latency         = timestamp - payload.timestamp
          nodeId          = ctx.config.nodeId
          storagePath     = ctx.config.storagePath
          evictedPath     = Paths.get(payload.evictedItemPath)
          _               <- liftFF[Unit](NIOFIles[IO].delete(evictedPath))
//          _               <- liftFF[Path](NIOFIles[IO].createDirectories(basePath))
          _               <- L.info(s"PULL_DONE_LATENCY ${payload.guid} $latency")
        } yield ()

        app.value.stopwatch.flatMap{ res =>
          res.result match {
            case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
            case Right(value) =>  for {
              _ <- acker.ack(envelope.deliveryTag)
              duration = res.duration.toMillis
              _ <-  ctx.logger.debug(s"PULL_DONE ${payload.guid} $duration")
            } yield ()
          }
        }
      }
      processMessageV2[IO,Payloads.PullDone,NodeContextV5](
        successCallback =  successCallback,
        errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
      )

  }

}
