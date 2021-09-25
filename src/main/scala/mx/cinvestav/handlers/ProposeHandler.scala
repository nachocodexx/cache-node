package mx.cinvestav.handlers

import cats.data.EitherT
import cats.implicits._
import cats.effect._
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpEnvelope, AmqpMessage, AmqpProperties}
import mx.cinvestav.Declarations.{CommandIds, NodeContextV5, NodeStateV5, Payloads, ProposedElement, liftFF}
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import org.typelevel.log4cats.Logger
import mx.cinvestav.utils.v2.encoders._
import io.circe.syntax._
import io.circe.generic.auto._
import mx.cinvestav.cache.cache.{CachePolicy, EvictedItem, EvictionResponse}
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.commons.{status=>StatuX}

object ProposeHandler {
  def apply()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val connection = rabbitMQContext.connection
      val client     = rabbitMQContext.client
      def successCallback(payload: Payloads.Propose) = {
        val app = for {
          timestamp           <- liftFF[Long](IO.realTime.map(_.toMillis))
          currentState        <- maybeCurrentState
          elements            <- liftFF[List[String]](currentState.currentEntries.get)
          replyTo             <- maybeReplyTo
          nodeId              = ctx.config.nodeId
          ip                  = currentState.ip
          port                = ctx.config.port
          cache               = currentState.cache
          cacheX              = CachePolicy(ctx.config.cachePolicy)
//          cacheSize           = ctx.config.cacheSize
//          elemLen             = elements.length
          latency             = timestamp - payload.timestamp
//         UPDATE STATUS
          _                   <- liftFF[Unit](ctx.state.update(_.copy(status = StatuX.Paused )))
//        ________________________________________________________________________________
          _                   <- L.info(s"PREPARE_LATENCY $latency")
          _                   <- L.debug(s"AFTER LATENCY")
          _                   <- L.debug(s"${payload.proposedElement}")
//        ____________________________________________________________________
          cacheNodes          = currentState.cacheNodePubs.filter(_._1==replyTo).values.toList
          x                   <- liftFF[EvictionResponse](cacheX.eviction(cache,remove=false))
          evicted             = x.evictedItem.getOrElse(EvictedItem.empty)
          _                   <- L.debug(s"EVICTED_ELEMENT ${evicted.key} ${evicted.value}")
          msgPayload          = Payloads.Promise(
            guid = payload.guid,
//            url = payload.url,
            timestamp = timestamp,
            proposedElement = ProposedElement(evicted.key,evicted.value),
            uploadUrl = s"http://$ip:$port/uploadv2"
          ).asJson.noSpaces
          properties          = AmqpProperties(
            headers = Map("commandId" -> StringVal(CommandIds.PROMISE)),
            replyTo = nodeId.some
          )
          msg                 = AmqpMessage[String](payload = msgPayload, properties = properties)
          (channel,finalizer) <-  liftFF[(AMQPChannel,IO[Unit])](client.createChannel(connection).allocated)
          implicit0(_channel:AMQPChannel) <- liftFF[AMQPChannel](IO.pure(channel))
          _ <- liftFF[List[Unit]] {
            cacheNodes.traverse { pub =>
              pub.publishWithChannel(msg)
            }
          }
//          CLOSE CHANNEL
          _            <- liftFF[Unit](finalizer)
          _            <- L.debug(s"CURRENT ELEMENTS: ${elements.mkString(",")}")
          _            <- L.debug(s"REPLY TO $replyTo with a PROMISE message")


        } yield ()

        app.value.stopwatch.flatMap{ res =>
          res.result match {
            case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
            case Right(value) =>  for {
              _ <-  ctx.logger.debug(s"PREPARE ${payload.guid}")
              _ <- acker.ack(envelope.deliveryTag)
            } yield ()
          }
        }
      }
      processMessageV2[IO,Payloads.Propose,NodeContextV5](
        successCallback =  successCallback,
        errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
      )

  }

}
