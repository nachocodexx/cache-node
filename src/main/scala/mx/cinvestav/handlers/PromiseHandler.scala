package mx.cinvestav.handlers

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpEnvelope, AmqpMessage, AmqpProperties}
import fs2.io.file.Files
import io.circe.{Encoder, Json}
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.Declarations._
import mx.cinvestav.cache.cache.{CachePolicy, CacheX, EvictedItem, EvictionResponse}
import mx.cinvestav.commons.errors.{NoReplyTo, NodeError}
import mx.cinvestav.commons.stopwatch.StopWatch._
import mx.cinvestav.utils.v2.encoders._
import mx.cinvestav.utils.v2.{Acker, processMessageV2}
import org.typelevel.log4cats.Logger

import java.nio.file.Paths
import cats.nio.file.{Files => NIOFIles}
import mx.cinvestav.Helpers
object PromiseHandler {

  implicit val cacheTransactionEncoder: Encoder[CacheTransaction] = (a: CacheTransaction) => Json.obj(
    ("id", Json.fromString(a.id) ),
    ("nodeId", a.nodeId.asJson),
    ("cacheNodes", a.cacheNodes.asJson),
    ("proposedElements", a.proposedElements.asJson),
    ("timestamp", a.timestamp.asJson),
    ("proposedElement", a.proposedElement.asJson),
    ("userId", a.userId.asJson),
    ("bucketName", a.bucketName.asJson)
  )

  def apply()(implicit ctx:NodeContextV5,envelope: AmqpEnvelope[String],acker:Acker) = {
    type E                = NodeError
    val maybeCurrentState = EitherT.liftF[IO,E,NodeStateV5](ctx.state.get)
    implicit val logger   = ctx.logger
    val L                 = Logger.eitherTLogger[IO,E]
    val unit                = liftFF[Unit](IO.unit)
    implicit val rabbitMQContext = ctx.rabbitMQContext
    val connection = rabbitMQContext.connection
    val client     = rabbitMQContext.client
    val maybeReplyTo      = EitherT.fromEither[IO](envelope.properties.replyTo.toRight{NoReplyTo()})
    def successCallback(payload: Payloads.Promise) = {
        val app = for {
          timestamp           <- liftFF[Long](IO.realTime.map(_.toMillis))
          currentState        <- maybeCurrentState
          proposedElement     = payload.proposedElement
          proposal            = CacheNodeProposal(proposedElement,payload.uploadUrl)
          nodeId              = ctx.config.nodeId
          storagePath         = ctx.config.storagePath
          replyTo             <- maybeReplyTo
          guid                = payload.guid
          latency             = timestamp - payload.timestamp
          _                   <- L.info(s"PROMISE_LATENCY ${payload.guid} $latency")
          transaction         <- EitherT.fromOption[IO].apply(currentState.transactions.get(payload.guid),TransactionNotFound(payload.guid))
           newTransaction  = transaction.copy(
            proposedElements = transaction.proposedElements + (replyTo -> proposal)
          )
          newCurrentState        <- liftFF[NodeStateV5]{
            ctx.state.updateAndGet{s=>
              val transactions = s.transactions
              s.copy(
               transactions = transactions + (guid -> newTransaction)
              )
            }
          }
//          newTransaction         <- EitherT.fromOption[IO].apply(newCurrentState.transactions.get(guid),TransactionNotFound(payload.guid))
          _                    <- L.debug(newTransaction.asJson(encoder=cacheTransactionEncoder).toString)
          _ <- if(newTransaction.proposedElements.size < newTransaction.cacheNodes.length) unit
          else for {
            _ <- L.debug("ALL CACHE_NODES PROMISE VALUES")
            userId          = newTransaction.userId
            bucketName      = newTransaction.bucketName
            filename        = newTransaction.filename
            baseStr         =  s"$storagePath/$nodeId/$userId/$bucketName"
            basePath        =  Paths.get(baseStr)
            _               <- liftFF[Unit](NIOFIles[IO].createDirectories(basePath).void)
            cache                   = newCurrentState.cache
            cacheX                  = CachePolicy(ctx.config.cachePolicy)
            firstProposedElem    = newTransaction.proposedElement
            proposedElements     = newTransaction.proposedElements
            emptySlots           = proposedElements.toList.filter(_._2.proposedElement.guid.isEmpty)
            _ <- if(emptySlots.isEmpty) for {
              _ <- L.debug("NO EMPTY SLOTS")
              filteredProposedElem = proposedElements.toList
                .filter(_._2.proposedElement.guid.nonEmpty)
                .filter(_._2.proposedElement.hits < firstProposedElem .hits)
                .minByOption(_._2.proposedElement.hits)
              _ <- filteredProposedElem match {
//              there's a lower element than the proposed
                case Some( (cacheNode,value)) =>  for {
                  _<- L.debug(s"SEND TO $cacheNode THE MIN PROPOSED ELEMENT $value")
                  _ <- liftFF[Unit](
                    Helpers.uploadToNode(
                      filename = filename,
                      guid = guid,
                      userId = userId.toString,
                      bucketName = bucketName,
                      url =  value.uploadUrl ,
                      body = newTransaction.data
                    )
                  )
                } yield ()
//              No of the proposed elements is lower
                case None =>  for {
                  _        <- L.debug(s"SEND FIRST PROPOSED $firstProposedElem")
                  sinkPath = Paths.get(baseStr,guid)
                  _        <- liftFF[Unit](newTransaction.data.through(Files[IO].writeAll(sinkPath)).compile.drain)
                  newCache <- liftFF[CacheX](cacheX.remove(cache,firstProposedElem.guid))
                  _        <- liftFF[Unit](ctx.state.update(s=>s.copy(cache= newCache)))
                } yield ()
              }
            } yield ()
//           There are empty slots in others cache nodes
            else for {
              _ <- L.debug("THERE's a empty slots")
//              _ <- L.debug(emptySlots.asJson.toString)
              _ <- liftFF[Unit](
                Helpers.uploadToNode(
                filename = filename,
                guid = guid,
                userId = userId.toString,
                bucketName = bucketName,
                url =  emptySlots.head._2.uploadUrl ,
                body = newTransaction.data
              )
              )
//              sinkPath    =  Paths.get(baseStr+s"/SOY_YO.pdf")
//              _ <- liftFF[Unit](newTransaction.data.through(Files[IO].writeAll(sinkPath)).compile.drain)
            } yield ()

          } yield ()

//          msgPayload          = Payloads.Accept(
//              guid            = payload.guid,
//              url             = payload.url,
//              timestamp       = timestamp,
//              proposedElement =  if(localProposedElement.hits < proposedElement.hits) localProposedElement else proposedElement
//            ).asJson.noSpaces
//          properties          = AmqpProperties(headers = Map("commandId" -> StringVal(CommandIds.ACCEPT)))
//          msg                 = AmqpMessage[String](payload = msgPayload, properties = properties)
//          (channel,finalizer) <-  liftFF[(AMQPChannel,IO[Unit])](client.createChannel(connection).allocated)
//          implicit0(_channel:AMQPChannel) <- liftFF[AMQPChannel](IO.pure(channel))
//          _ <- liftFF[List[Unit]] {
//            cacheNodes.traverse { pub =>
//              pub.publishWithChannel(msg)
//            }
//          }
////          CLOSE CHANNEL
//          _ <- liftFF[Unit](finalizer)
        } yield ()

        app.value.stopwatch.flatMap{ res =>
          res.result match {
            case Left(e) => acker.reject(envelope.deliveryTag) *> ctx.logger.error(e.getMessage)
            case Right(value) =>  for {
              _ <- acker.ack(envelope.deliveryTag)
              duration = res.duration.toMillis
              _ <-  ctx.logger.debug(s"PROMISE ${payload.guid} $duration")
            } yield ()
          }
        }
      }
      processMessageV2[IO,Payloads.Promise,NodeContextV5](
        successCallback =  successCallback,
        errorCallback   = e=>ctx.logger.error(e.getMessage) *> acker.reject(envelope.deliveryTag)
      )

  }

}
