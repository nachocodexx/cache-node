package mx.cinvestav.handlers

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpEnvelope, AmqpMessage, AmqpProperties}
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.Declarations._
import mx.cinvestav.Helpers
import mx.cinvestav.cache.cache.{CachePolicy, EvictedItem, EvictionResponse}
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.utils.v2.encoders._
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.{status => StatuX}

object PrepareHandler {
  def apply()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
      type E                = NodeError
      val maybeCurrentState = liftFF[NodeStateV5](ctx.state.get)
      val unit = liftFF[Unit](IO.unit)
      implicit val logger   = ctx.logger
      val maybeReplyTo:EitherT[IO,E,String] = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val connection = rabbitMQContext.connection
      val client     = rabbitMQContext.client
      def successCallback(payload: Payloads.Prepare) = {
        val app = for {
          timestamp           <- liftFF[Long](IO.realTime.map(_.toMillis))
          currentState        <- maybeCurrentState
          replyTo             <- maybeReplyTo
          operationId         = payload.operationId
          currentOperationId  = currentState.currentOperationId
          nodeId              = ctx.config.nodeId
          ip                  = currentState.ip
          port                = ctx.config.port
          latency             = timestamp - payload.timestamp
          cache               = currentState.cache
          cacheX              = CachePolicy(ctx.config.cachePolicy)
          _ <- currentOperationId match {
            case Some(value) if(value > operationId)=> for {
              _ <- L.debug(s"THERES a greater transaction for operationId $value > $operationId")
            } yield ()
            case Some(value) if(value < operationId)=> for {
              _ <- L.debug(s"THERES a lower transaction for operationId $value < $operationId")
            } yield ()
            case None => for {
              _          <- L.debug(s"PREPARE_LATENCY $latency")
              cacheNodes = currentState.cacheNodePubs.filter(_._1==replyTo).values.toList
              x          <- liftFF[EvictionResponse](cacheX.eviction(cache,remove=false))
              evicted    = x.evictedItem.getOrElse(EvictedItem.empty)
              _          <- L.debug(s"EVICTED_ELEMENT ${evicted.key} ${evicted.value}")
              _          <- liftFF[Unit](ctx.state.update(_.copy(
                currentOperationId = operationId.some,
                status = StatuX.Paused
              )))
              _ <- liftFF[Unit](Helpers.sendPromise(nodeId,payload,evicted,replyTo))
            } yield ()
          }
        } yield ()

        app.value.stopwatch.flatMap{ res =>
          res.result match {
            case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
            case Right(value) =>  for {
              _ <-  ctx.logger.debug(s"PREPARE ${payload.guid} ${res.duration}")
              _ <- acker.ack(envelope.deliveryTag)
            } yield ()
          }
        }
      }
      processMessageV2[IO,Payloads.Prepare,NodeContextV5](
        successCallback =  successCallback,
        errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
      )

  }

}
