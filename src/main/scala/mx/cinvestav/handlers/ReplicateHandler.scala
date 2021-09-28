package mx.cinvestav.handlers

import cats.data.EitherT
import cats.effect._
import dev.profunktor.fs2rabbit.model.AmqpEnvelope
import io.circe.generic.auto._
import mx.cinvestav.Declarations._
import mx.cinvestav.cache.cache.CachePolicy
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import org.typelevel.log4cats.Logger

object ReplicateHandler {
  def apply()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val connection = rabbitMQContext.connection
      val client     = rabbitMQContext.client
      def successCallback(payload: Payloads.Accept) = {
        val app = for {
          timestamp           <- liftFF[Long](IO.realTime.map(_.toMillis))
          currentState        <- maybeCurrentState
          proposedElement     = payload.proposedElement
          replyTo             <- maybeReplyTo
          latency             = timestamp - payload.timestamp
          _                   <- L.info(s"REPLICATE ${payload.guid} $latency")
        } yield ()

        app.value.stopwatch.flatMap{ res =>
          res.result match {
            case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
            case Right(value) =>  for {
              _ <- acker.ack(envelope.deliveryTag)
              duration = res.duration.toMillis
              _ <-  ctx.logger.debug(s"ACCEPT ${payload.guid} $duration")
            } yield ()
          }
        }
      }
      processMessageV2[IO,Payloads.Accept,NodeContextV5](
        successCallback =  successCallback,
        errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
      )

  }

}
