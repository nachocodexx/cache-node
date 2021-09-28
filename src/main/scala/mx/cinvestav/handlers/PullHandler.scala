package mx.cinvestav.handlers

import cats.implicits._
import cats.effect._
import cats.data.EitherT
import cats.nio.file.{Files => NIOFIles}
import mx.cinvestav.cache.cache.PutResponse
//
import dev.profunktor.fs2rabbit.model.{AmqpEnvelope, AmqpMessage, AmqpProperties}
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.Declarations._
import mx.cinvestav.Helpers
import mx.cinvestav.cache.cache.CachePolicy
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.server.Client
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import org.typelevel.log4cats.Logger

import java.nio.file.{Path, Paths}
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import mx.cinvestav.commons.compression
import mx.cinvestav.utils.v2.encoders._

object PullHandler {
  def apply()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val connection = rabbitMQContext.connection
      val client     = rabbitMQContext.client
      def successCallback(payload: Payloads.Pull) = {
        val app = for {
          timestamp       <- liftFF[Long](IO.realTime.map(_.toMillis))
          currentState    <- maybeCurrentState
          cache           = currentState.cache
          cacheX          = CachePolicy(ctx.config.cachePolicy)
          //
          replyTo         <- maybeReplyTo
          replyToPub      = currentState.cacheNodePubs.get(replyTo)
          replyToPub2     = currentState.syncNodePubs.get(replyTo)
//
          latency         = timestamp - payload.timestamp
          nodeId          = ctx.config.nodeId
          storagePath     = ctx.config.storagePath
          userId          =  payload.userId
          bucketName      =  payload.bucketName
          guid            = payload.guid
          baseStr         =  s"$storagePath/$nodeId/$userId/$bucketName"
          basePath        =  Paths.get(baseStr)
          _               <- liftFF[Path](NIOFIles[IO].createDirectories(basePath))
          _               <- L.info(s"PULL_LATENCY ${payload.guid} $latency")
          ca              = compression.fromString(payload.compressionAlgorithm)
          sinkPath        = basePath.resolve(guid+s".${ca.extension}")
          putResponse <- liftFF[PutResponse]{
            cacheX.put(cache,guid)
          }
          _ <- liftFF[Unit](ctx.state.update(_.copy(
            cache = putResponse.newCache
          )))
          _ <- putResponse.evicted match {
            case Some(value) => for {
              _ <- L.debug(s"SEND $value TO THE CLOUD")
            } yield ()
            case None =>  for {
              _               <- liftFF[Unit](Client.downloadFileV3(userId,bucketName,payload.url,sinkPath))
              //        SEND DONE
              publisher       = replyToPub orElse replyToPub2
              pullDonePayload = Payloads.PullDone(guid=payload.guid,evictedItemPath = payload.evictedItemPath,timestamp=timestamp)
              properties      = AmqpProperties(headers = Map("commandId"->StringVal(CommandIds.PULL_DONE)))
              message         = AmqpMessage[String](payload=pullDonePayload.asJson.noSpaces,properties = properties)
              x               <- liftFF[Option[Unit]](publisher.traverse(_.publish(message)))
            } yield ()
          }
//        DOWNLOAD
        } yield ()

        app.value.stopwatch.flatMap{ res =>
          res.result match {
            case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
            case Right(value) =>  for {
              _ <- acker.ack(envelope.deliveryTag)
              duration = res.duration.toMillis
              _ <-  ctx.logger.debug(s"PULL ${payload.guid} $duration")
            } yield ()
          }
        }
      }
      processMessageV2[IO,Payloads.Pull,NodeContextV5](
        successCallback =  successCallback,
        errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
      )

  }

}
