package mx.cinvestav
import cats.effect.kernel.Outcome

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.net.URL
import cats.implicits._
import cats.effect.{IO, Ref}
import com.dropbox.core.v2.files.FileMetadata
import com.github.gekomad.scalacompress.CompressionStats
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpMessage, AmqpProperties, ExchangeName, ExchangeType, RoutingKey}
import fs2.Pipe
import fs2.io.file.Files
import mx.cinvestav.Declarations.{CacheTransaction, CommandIds, DownloadError, NodeContextV5, NodeContextV6, ObjectS, Payloads, ProposedElement, StorageNode, User, liftFF}
import mx.cinvestav.cache.cache.{CachePolicy, EvictedItem}
import mx.cinvestav.cache.CacheX.{EvictedItem => EvictedItemV2}
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.events.{Del, EventX, Get, Push, Put, Pull => PullEvent}
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.PushResponse
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}
//import mx.cinvestav.server.HttpServer.User
import mx.cinvestav.server.Routes.UploadResponse
import mx.cinvestav.utils.v2.{PublisherConfig, PublisherV2, RabbitMQContext}
import org.http4s.Request
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci._
//
import concurrent.duration._
import language.postfixOps
//
import io.circe.syntax._
import io.circe.generic.auto._
import scala.language.postfixOps
import mx.cinvestav.utils.v2.encoders._
import org.apache.commons.io.FileUtils
import mx.cinvestav.commons.payloads.{v2=>PAYLOADS}
import org.http4s._
import fs2.Stream
import cats.nio.file.{Files => NIOFIles}
//

object Helpers {

  import java.util.zip.DeflaterOutputStream
  import java.util.zip.InflaterOutputStream

  def compress(in: Array[Byte]):IO[Array[Byte]] = {
    for {
      _    <- IO.unit
      out  = new ByteArrayOutputStream()
      defl = new DeflaterOutputStream(out)
      _    <- IO.delay(defl.write(in))
      _    <- IO.delay(defl.flush())
      _    <- IO.delay(defl.close())
    } yield out.toByteArray
  }

  def decompress(in: Array[Byte]): IO[Array[Byte]] = for {
    _    <- IO.unit
    out  = new ByteArrayOutputStream()
    infl = new InflaterOutputStream(out)
    _    <- IO.delay(infl.write(in))
    _    <- IO.delay(infl.flush())
    _    <- IO.delay(infl.close())
  } yield out.toByteArray

  def pushToCloud(evictedObject:ObjectS,currentEvents:List[EventX],correlationId:String="")(implicit ctx:NodeContextV6): IO[Unit] = {
    //  IO[List[EventX]] = {
    for {

      currentState  <- ctx.state.get
      alreadyPushed = Events.alreadyPushedToCloud(objectId = evictedObject.guid,events = currentEvents)
      pushEvents             <- if(!alreadyPushed)
      {
        for {

          pushStartAtNanos       <- IO.monotonic.map(_.toNanos)
          evictedObjectExtension = evictedObject.metadata.getOrElse("extension","bin")
          evictedObjectId        = evictedObject.guid
          evictedObjectFilename  = s"${evictedObjectId}.${evictedObjectExtension}"
          fileExits              <- Dropbox.fileExists(currentState.dropboxClient)(filename = evictedObjectFilename)
          retryPolicy            = RetryPolicies.limitRetries[IO](10) join RetryPolicies.exponentialBackoff[IO](10 seconds)
          uploadIO               = Dropbox.uploadObject(currentState.dropboxClient)(
            filename = s"$evictedObjectId.$evictedObjectExtension",
            in = new ByteArrayInputStream(evictedObject.bytes)
          )
          _                      <- if(!fileExits) retryingOnAllErrors[FileMetadata](
            policy = retryPolicy,
            onError = (e:Throwable,d:RetryDetails) => ctx.errorLogger.error(e.getMessage)
          )(uploadIO)
          else IO.unit
          pushEndAt              <- IO.realTime.map(_.toMillis)
          pushEndAtNanos         <- IO.monotonic.map(_.toNanos)
          serviceTimePushNanos   =  pushEndAtNanos - pushStartAtNanos
          //
          pushEvent = Push(
            serialNumber     = 0,
            nodeId           = ctx.config.nodeId,
            objectId         = evictedObject.guid,
            objectSize       = evictedObject.bytes.length,
            pushTo           = "Dropbox",
            timestamp        = pushEndAt,
            serviceTimeNanos = serviceTimePushNanos,
            correlationId    = correlationId
          )
          _ <- Events.saveEvents(events = List(pushEvent))
          _   <- ctx.logger.info(s"PUSH ${evictedObject.guid} ${evictedObject.bytes.length} $serviceTimePushNanos $correlationId")
        } yield ()
//          List(pushEvent)
      }
//      else IO.pure(List.empty[Push]) <* ctx.logger.info(s"ALREADY_PUSHED ${evictedObject.guid} ${evictedObject.bytes.length} 1")
      else ctx.logger.info(s"ALREADY_PUSHED ${evictedObject.guid} ${evictedObject.bytes.length} 1 $correlationId")
    } yield ()
//      pushEvents
  }


}
