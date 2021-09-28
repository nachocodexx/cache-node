package mx.cinvestav

import cats.data.EitherT
import cats.effect.IO
import cats.implicits._
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpEnvelope, AmqpFieldValue, AmqpMessage, AmqpProperties, ExchangeName, RoutingKey}
import mx.cinvestav.Declarations.{BadArguments, CommandIds, NodeContextV5, NodeStateV5, liftFF}
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.commons.fileX.{FileMetadata => Metadata}
import mx.cinvestav.commons.compression
import mx.cinvestav.server.Client
import mx.cinvestav.utils.v2._

import java.net.URL
import java.nio.file.Paths
//
import io.circe.syntax._
import io.circe.generic.auto._
import io.circe.generic.semiauto.deriveDecoder
import io.circe.Decoder
//
import mx.cinvestav.commons.payloads
import mx.cinvestav.commons.payloads.{v2 => PAYLOADS}
import mx.cinvestav.Declarations.Payloads
//
import org.typelevel.log4cats.Logger
import mx.cinvestav.commons.stopwatch.StopWatch._
import scala.language.postfixOps
import mx.cinvestav.utils.v2.encoders._

object CommandHandlers {
  implicit val startHeartbeatPayloadDecoder:Decoder[payloads.StartHeartbeat] = deriveDecoder
  implicit val stopHeartbeatPayloadDecoder:Decoder[payloads.StopHeartbeat] = deriveDecoder
  implicit val newCoordinatorPayloadDecoder:Decoder[payloads.NewCoordinator] = deriveDecoder
  implicit val newCoordinatorV2PayloadDecoder:Decoder[payloads.NewCoordinatorV2] = deriveDecoder




//  def propose()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
//    type E                = NodeError
//    val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
//    val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
//    implicit val logger   = ctx.logger
//    val L                 = Logger.eitherTLogger[IO,E]
//    implicit val rabbitMQContext = ctx.rabbitMQContext
//    val connection = rabbitMQContext.connection
//    val client     = rabbitMQContext.client
//    def successCallback(payload: Payloads.Propose) = {
//      val app = for {
//        timestamp           <- liftFF[Long](IO.realTime.map(_.toMillis))
//        currentState        <- maybeCurrentState
//        latency             = timestamp - payload.timestamp
//        cacheNodes          = currentState.cacheNodePubs.values.toList
//        msgPayload          = Payloads.Prepare(
//          guid = payload.guid,
//          url = payload.url,
//          timestamp = timestamp,
//          proposedElement =
//        ).asJson.noSpaces
//        properties          = AmqpProperties(headers = Map("commandId" -> StringVal(CommandIds.PREPARE)))
//        msg                 = AmqpMessage[String](payload = msgPayload, properties = properties)
//        (channel,finalizer) <-  liftFF[(AMQPChannel,IO[Unit])](client.createChannel(connection).allocated)
//        implicit0(_channel:AMQPChannel) <- liftFF[AMQPChannel](IO.pure(channel))
//        _ <- liftFF[List[Unit]] {
//          cacheNodes.traverse { pub =>
//            pub.publishWithChannel(msg)
//          }
//        }
//        _ <- liftFF[Unit](finalizer)
//        _            <- L.info(s"PROPOSE_LATENCY $latency")
//      } yield ()
//
//      app.value.stopwatch.flatMap{ res =>
//        res.result match {
//          case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
//          case Right(value) =>  for {
//            _ <-  ctx.logger.debug(s"PROPOSE ${payload.guid} ${res.duration}")
//            _ <- acker.ack(envelope.deliveryTag)
//          } yield ()
//        }
//      }
//    }
//    processMessageV2[IO,Payloads.Propose,NodeContextV5](
//      successCallback =  successCallback,
//      errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
//    )
//  }

  def uploadParts()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
    def successCallback(payload: PAYLOADS.UploadV2) = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val app = for {
        currentState <- maybeCurrentState
        timestamp    <- liftFF[Long](IO.realTime.map(_.toMillis))
        latency      = timestamp - payload.timestamp
//
        nodeId       = ctx.config.nodeId
        poolId       = ctx.config.poolId
        ip           = currentState.ip
        port         = ctx.config.port
        storagePath  = ctx.config.storagePath
//        keystore     = currentState.keyStore
        locations    = payload.locations
        guid         = payload.guid

//
        sinkFolder   = Paths.get(s"$storagePath/$nodeId/$guid")
        sinkChunkFolder= sinkFolder.resolve("chunks")
        _            <- liftFF[Unit](IO.delay{sinkFolder.toFile.mkdirs()})
        _            <- liftFF[Unit](IO.delay{sinkChunkFolder.toFile.mkdirs()})
        _            <- L.info(s"UPLOAD_LATENCY $latency")
        _            <- L.debug(s"NUM_PARTS ${payload.locations.length}")
        urls         = locations.map(new URL(_))
        paths        = urls.map(_.getPath).map(Paths.get(_))
        metadata     = paths.map(Metadata.fromPath)
        destinations = metadata.map(x=>s"$sinkChunkFolder/${x.fullname}").map(Paths.get(_))
        _            <- liftFF[List[Unit]](
          (locations zip destinations).traverse{
            case (location, destination) =>
              Client.downloadFilev2(location,destination)
          }
        )
        _            <- L.debug("DOWNLOADS....")
//       DECOMPRESS
        ca           = compression.fromString(payload.compressionAlgorithm)

        _            <- L.debug("MERGE")
        _            <- L.debug("SEND_METADTA TO CHORD")
      } yield()
      app.value.stopwatch.flatMap{ res=>
        res.result match {
          case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) => for {
            _        <- acker.ack(envelope.deliveryTag)
            duration = res.duration
            guid     = payload.guid
            _        <- ctx.logger.info(s"UPLOAD_PARTS $guid $duration")
          } yield ()
        }
      }
    }
    processMessageV2[IO,PAYLOADS.UploadV2,NodeContextV5](
      successCallback =  successCallback,
      errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }
//

  def addNode()(implicit ctx:NodeContextV5, envelope: AmqpEnvelope[String], acker:Acker):IO[Unit] = {
    def successCallback(acker:Acker,envelope: AmqpEnvelope[String],payload: Payloads.AddStorageNode):IO[Unit] = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      implicit val rabbitMQContext = ctx.rabbitMQContext
      val app = for {
        _         <- EitherT.fromEither[IO](Either.unit[NodeError])
        predicate = payload.storageNodeId!=ctx.config.nodeId
        check     = Option.when(predicate)(())
//        _         <- L.info(check.toString+s" ${payload.storageNodeId} != ${ctx.config.nodeId} -> $predicate")
//        _ <- EitherT.fromEither[IO](Either.fromOption(check,  BadArguments("Storage node cannot add itself")  ))
        storageNodeId   = payload.storageNodeId
        poolId          = ctx.config.poolId
        nodeId          = ctx.config.nodeId
        routingKey      = RoutingKey(s"$poolId.$nodeId")
        exchangeName    = ExchangeName(poolId)
        publisherConfig = PublisherConfig(exchangeName=exchangeName,routingKey=routingKey)
        publisher       = PublisherV2(publisherConfig = publisherConfig)
        currentState    <- liftFF[NodeStateV5](ctx.state.updateAndGet(
          s=>s.copy(
            cacheNodes      = s.cacheNodes :+ storageNodeId,
            cacheNodePubs   = s.cacheNodePubs + (storageNodeId -> publisher),
            availableResources = s.availableResources+1
          )
        )
        )
        _ <- L.debug(currentState.toString)
      } yield()
//    __________________________________________________________________
      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(value) =>
            acker.ack(deliveryTag = envelope.deliveryTag) *> ctx.logger.info(s"${CommandIds.ADD_NODE} ${payload.storageNodeId}")
        }
      }
    }

    processMessage[IO,Payloads.AddStorageNode,NodeContextV5](
      successCallback =  successCallback,
      errorCallback   =  (acker,envelope,e)=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }
  def removeNode()(implicit ctx:NodeContextV5, envelope: AmqpEnvelope[String], acker:Acker):IO[Unit] = {
    def successCallback(acker:Acker,envelope: AmqpEnvelope[String],payload: Payloads.RemoveStorageNode):IO[Unit] = {
      type E                = NodeError
      val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
      implicit val logger   = ctx.logger
      val L                 = Logger.eitherTLogger[IO,E]
      val app = for {
        _ <- EitherT.fromEither[IO](Either.unit[NodeError])
//      _________________________________________________________________________________________
        nodeId        = ctx.config.nodeId
        storageNodeId = payload.storageNodeId
        predicate     = payload.storageNodeId!=ctx.config.nodeId
        check         = Option.when(predicate)(())
        error         = BadArguments("Storage node cannot remove itself")
//      _________________________________________________________________________________
        _ <- L.debug(check.toString+s" $storageNodeId != $nodeId -> $predicate")
        _ <- EitherT.fromEither[IO](Either.fromOption(check,error))
//      ________________________________________________________________________________
        currentState <- liftFF[NodeStateV5](ctx.state.updateAndGet(
          s=>s.copy(
            cacheNodes      = s.cacheNodes.filter(_!=storageNodeId),
            cacheNodePubs   = s.cacheNodePubs.filter(_._1!=storageNodeId),
            availableResources = s.availableResources-1
          )
        )
        )
//      ________________________________________________________________________________
        _ <- L.debug(currentState.toString)
      } yield()
      app.value.stopwatch.flatMap { result =>
        result.result match {
          case Left(e) =>  acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
          case Right(_) => for {
            _ <-acker.ack(deliveryTag = envelope.deliveryTag)
            storageNodeId = payload.storageNodeId
            _ <- ctx.logger.info(s"${CommandIds.REMOVE_NODE} $storageNodeId")
          } yield ()
        }
      }
    }
    processMessage[IO,Payloads.RemoveStorageNode,NodeContextV5](
      successCallback =  successCallback,
      errorCallback   =  (acker,envelope,e)=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
    )
  }





}
