package mx.cinvestav
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import cats.implicits._
import cats.effect.{IO, Ref}
import com.dropbox.core.v2.files.FileMetadata
import fs2.io.file.Files
import mx.cinvestav.Declarations.NodeContextV6
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.{EventX,Push}
import mx.cinvestav.events.Events
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}

import java.nio.file.Paths
import concurrent.duration._
import language.postfixOps
import io.circe.syntax._
import io.circe.generic.auto._
import scala.language.postfixOps
import org.http4s._
import fs2.Stream

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

//  def pushToCloud(evictedObject:ObjectS,currentEvents:List[EventX],correlationId:String="")(implicit ctx:NodeContextV6): IO[Unit] = {
    def pushToNextLevel(
                     evictedObjectId:String,
                     bytes:Array[Byte],
                     metadata:Map[String,String],
                     currentEvents:List[EventX],
                     userId:String = "",
                     correlationId:String="",
                     delete:Boolean = false,
                   )(implicit ctx:NodeContextV6): IO[Unit] = {
    //  IO[List[EventX]] = {
    for {

      currentState  <- ctx.state.get
      alreadyPushed = if(ctx.config.cloudEnabled) Events.alreadyPushedToCloud(objectId = evictedObjectId,events = currentEvents) else false
      objectSize = bytes.length
      pushEvents             <- if(!alreadyPushed)
      {
        for {

          pushStartAtNanos       <- IO.monotonic.map(_.toNanos)
          evictedObjectExtension = metadata.getOrElse("extension","bin")
//          evictedObjectId        = evictedObjectId
          evictedObjectFilename  = evictedObjectId
//            s"${evictedObjectId}.${evictedObjectExtension}"
           x <- if(ctx.config.cloudEnabled) {
             for {
               fileExits              <- Dropbox.fileExists(currentState.dropboxClient)(filename = evictedObjectFilename)
               retryPolicy            = RetryPolicies.limitRetries[IO](10) join RetryPolicies.exponentialBackoff[IO](10 seconds)
               uploadIO  = Dropbox.uploadObject(currentState.dropboxClient)(
                 filename = evictedObjectFilename,
//                   s"$evictedObjectId.$evictedObjectExtension",
                 in = new ByteArrayInputStream(bytes)
               )
               sendToCloud = retryingOnAllErrors[FileMetadata](
                 policy = retryPolicy,
                 onError = (e:Throwable,d:RetryDetails) => ctx.errorLogger.error(e.getMessage)
               )(uploadIO)
               _         <- if(!fileExits) sendToCloud else
                 ctx.logger.info(s"ALREADY_PUSHED ${evictedObjectId} $objectSize CLOUD $correlationId")
             } yield ("","")
           }
          else {
             for {
               _ <- ctx.logger.debug(s"PUSH_NEX_LEVEL $evictedObjectId $correlationId")
               x <- ctx.config.cachePool.upload(
                 objectId = evictedObjectId,bytes = bytes,
                 userId=userId,
                 operationId = correlationId,
                 contentType = MediaType.forExtension(evictedObjectExtension).getOrElse(MediaType.application.`octet-stream`)
               ).onError{ e=>
                 ctx.errorLogger.error(e.getMessage)
               }
             } yield x
           }
          pushEndAt              <- IO.realTime.map(_.toMillis)
          pushEndAtNanos         <- IO.monotonic.map(_.toNanos)
          serviceTimePushNanos   =  pushEndAtNanos - pushStartAtNanos
          //
          pushEvent = Push(
            serialNumber     = 0,
            nodeId           = x._1,
            objectId         = evictedObjectId,
            objectSize       = objectSize,
            pushTo           = if(ctx.config.cloudEnabled) "cloud" else "cache-pool",
            timestamp        = pushEndAt,
            serviceTimeNanos = serviceTimePushNanos,
            correlationId    = correlationId,
            uri              = x._2
          )
          _ <- if(!ctx.config.cloudEnabled && x._1.isEmpty && x._2.isEmpty)
            ctx.logger.info(s"ALREADY_PUSHED ${evictedObjectId} $objectSize CACHE $correlationId")
          else for {
            _ <- Events.saveEvents(events = List(pushEvent))
            _   <- ctx.logger.info(s"PUSH ${evictedObjectId} $objectSize $serviceTimePushNanos $correlationId")
          } yield ()
        } yield ()
      }
      else ctx.logger.info(s"ALREADY_PUSHED ${evictedObjectId} $objectSize CLOUD $correlationId")
      _ <-  if(delete) Files[IO].delete(Paths.get(s"${ctx.config.storagePath}/$evictedObjectId")) else IO.unit
    } yield ()
//      pushEvents
  }


}
