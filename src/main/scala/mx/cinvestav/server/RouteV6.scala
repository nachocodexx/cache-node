package mx.cinvestav.server
// Cats
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.{NodeContextV6, objectSEncoder}
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.Del
import org.http4s.{MediaType, Method}
import org.http4s.blaze.client.BlazeClientBuilder

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.concurrent.ExecutionContext.global
// Cats NIO
import cats.nio.file.{Files=>NIOFiles}
// Fs2
import fs2.io.file.Files
// Local
import mx.cinvestav.Declarations.{NodeContextV5, ObjectS, User}
import mx.cinvestav.events.Events
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.{Put,Get,Pull=>PullEvent,EventXOps}
// Http4s
import org.http4s.{Header, Headers, Request, Uri}
import org.http4s.{headers=>HEADERS}
import org.http4s.multipart.{Multipart, Part}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.AuthedRoutes
import org.typelevel.ci.CIString
import org.http4s.implicits._
import org.http4s.dsl.io._
// Circe
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
// Native
import java.nio.file.Paths
import java.util.UUID

object RouteV6 {

  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           milliSeconds:Long,
                           timestamp:Long,
                           level:Int
//                           evictedItem
                         )

  def apply()(implicit ctx:NodeContextV6): AuthedRoutes[User, IO] =  AuthedRoutes.of[User,IO]{
    case authReq@POST -> Root / "upload" as user => for {
//      arrivalTime  <- IO.realTime.map(_.toMillis)
      currentState <- ctx.state.get
//      currentLevel = ctx.config.level
//      currentNodeId = ctx.config.nodeId
//      cache        = currentState.cacheX
      req          = authReq.req
      multipart    <- req.as[Multipart[IO]]
      parts        = multipart.parts
//      levelId      = currentState.levelId
//    _______________________________________________
      responses    <- parts.traverse{ part =>
        for{
          _           <- ctx.logger.debug(part.headers.toString)
          partHeaders = part.headers
          guid        = partHeaders.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          contentType = partHeaders.get(HEADERS.`Content-Type`.headerInstance.name).map(_.head.value).getOrElse("application/octet-stream")
          media       = MediaType.unsafeParse(contentType)
          objectExtension = media.fileExtensions.head
          body        = part.body
          bytesBuffer <- body.compile.to(Array)
//            .through(Helpers.streamBytesToBuffer)
          rawBytesLen  = bytesBuffer.length
          _ <- ctx.logger.debug(s"RAW_BYTES $rawBytesLen")
          objectSize  = partHeaders
            .get(org.http4s.headers.`Content-Length`.name)
            .map(_.head.value)
            .getOrElse("0")

          value       = ObjectS(
            guid=guid,
            bytes= bytesBuffer,
            metadata=Map(
              "objectSize"->objectSize,
              "contentType" -> contentType,
              "extension" -> objectExtension
            )
          )
          //        PUT TO CACHE
          response <- Helpers.putInCacheUpload(guid= guid, value = value)
        } yield response
      }.map(_.head)
      _ <- ctx.logger.debug(responses.toString)
      _ <- ctx.logger.debug("____________________________________________________")
    } yield responses
    case authReq@POST -> Root / "replicate" / UUIDVar(guid) as user => for {
      currentState <- ctx.state.get
      cacheX               = currentState.cacheX
      req                  = authReq.req
      headers              = req.headers
//
      replicationStrategy  = headers.get(CIString("Replication-Strategy")).map(_.head.value).getOrElse("ACTIVE")
      replicationFactor    = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
      replicaNodes         = headers.get(CIString("Replica-Node")).map(_.map(_.value))
//
      _                    <- ctx.logger.debug(s"REPLICATION_STRATEGY $replicationStrategy")
      _                    <- ctx.logger.debug(s"REPLICATION_FACTOR $replicationFactor")
      _                    <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
//
      getResponse          <- cacheX.get(guid.toString)
      newRes       <- getResponse.item match {
        case Some(value) =>
          if(replicationStrategy == "ACTIVE") for {
            _              <- ctx.logger.debug("ACTIVE_REPLICATION")
            objectX        = value.value
            streamBytes    = fs2.Stream.emits(objectX.bytes).covary[IO]
            objectSize     = objectX.bytes.length
            multipart      = Multipart[IO](parts = Vector(Part[IO](
              headers =  Headers(
                HEADERS.`Content-Length`(objectSize),
                HEADERS.`Content-Type`(MediaType.application.pdf),
                Header.Raw(CIString("guid"),guid.toString),
                Header.Raw(CIString("filename"),"default")
              ),
              body    =  streamBytes
            )))
            requests      = replicaNodes.map{ urls =>
              urls.map( url =>
                Request[IO](
                  method  = Method.POST,
                  uri     = Uri.unsafeFromString(url),
                  headers =  multipart.headers
                )
                  .withEntity(multipart)
                  .putHeaders(
                    Headers(
                      Header.Raw(CIString("User-Id"),user.id.toString),
                      Header.Raw(CIString("Bucket-Id"),user.bucketName)
                    ),
                  )
              )
            }
            (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
            responses          <- requests.traverse(_.traverse(req=>client.toHttpApp.run(req)))
            _                  <- ctx.logger.debug(responses.toString)
            _                  <- finalizer
            res                <- Ok("")
          } yield res
          else for {
            _   <- ctx.logger.debug("PASSIVE_REPLICATION")
            res <- Ok("")
          } yield res
        case None => NotFound()
      }
      response <- Ok("REPLICATE")
    } yield response

    case authReq@GET -> Root / "download" / UUIDVar(guid) as user => for {
      arrivalTime   <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      currentLevel  = ctx.config.level
      currentNodeId = ctx.config.nodeId
      cache         = currentState.cacheX
      req           = authReq.req
      headers       = req.headers
      objectExt     = headers.get(CIString("Object-Extension")).map(_.head.value).getOrElse("")
      levelId       = currentState.levelId
      getResponse   <- cache.get(guid.toString)
      response      <- getResponse.item match {
        case Some(element) => for {
          _            <- ctx.logger.info(s"GET $guid $levelId ${element.counter}")
          rawBytes = element.value.bytes
          streamBytes  = fs2.Stream.emits(rawBytes).covary[IO]
          bytesLen     =  rawBytes.length
          _            <- ctx.logger.debug(s"RAW_BYTES $bytesLen")
          getServiceTime <- IO.realTime.map(_.toMillis).map(_ - arrivalTime)
          _            <- ctx.state.update{ s=>
            val event = Get(
              nodeId       = currentNodeId,
              eventId      = UUID.randomUUID().toString,
              serialNumber = s.events.length,
              objectId     = guid.toString,
              objectSize   = bytesLen,
              timestamp    = arrivalTime,
              milliSeconds = getServiceTime
            )
            s.copy(events = s.events :+ event)
          }
          response <- Ok(
            streamBytes,
            Headers(
              Header.Raw(CIString("Object-Id"), element.value.guid),
              Header.Raw(CIString("Object-Size"), bytesLen.toString ),
              Header.Raw(CIString("Level"),currentLevel.toString),
              Header.Raw(CIString("Node-Id"),ctx.config.nodeId)
            ),
          )

        } yield response
        case None => for {
//          _            <- ctx.logger.info(s"MISS $guid")
          _            <- IO.unit
          filename     = s"${guid}.$objectExt"
          //          _ <- ctx.logger.debug(s"FILENAME $filename")
          //       Download cloud
          cloudStartAt  <- IO.realTime.map(_.toMillis)
          out           = new ByteArrayOutputStream()
          elementBytes  <- Dropbox.downloadObject(currentState.dropboxClient)(filename=filename,out=out )
          cloudEndAt    <- IO.realTime.map(_.toMillis)
          pullServiceTime = cloudEndAt - cloudStartAt
          _            <- ctx.logger.info(s"PULL $guid $pullServiceTime")
          //
          newObject    = ObjectS(
            guid= guid.toString,
            bytes = elementBytes,
            metadata = Map(
              "objectSize" ->elementBytes.length.toString,
              "contentType" -> "application/octet-stream",
              "extension"  -> objectExt
            )
          )
          //
          pullEvent = PullEvent(
            eventId = UUID.randomUUID().toString,
            serialNumber = 0,
            nodeId = currentNodeId,
            objectId = guid.toString,
            objectSize = elementBytes.length,
            pullFrom = "Dropbox",
            milliSeconds = pullServiceTime,
            timestamp = arrivalTime,
          )

          newHeaders <- Helpers.putInCacheDownload(arrivalTime = arrivalTime,guid= guid.toString,value=newObject,pullEvent= pullEvent)
//          _ <- ctx.logger.debug(s"OUT_LEN ${out.toByteArray.length}")
//          _ <- ctx.logger.debug(s"ELEMENT_BYTES ${elementBytes.length}")
          streamBytes  = fs2.Stream.emits(elementBytes).covary[IO]
          response <- Ok(streamBytes, newHeaders)
        } yield response
      }

//      _ <- ctx.logger.debug("RESPONSE -> "+response.toString)
      _ <- ctx.logger.debug("____________________________________________________")
    } yield response

    case authReq@POST -> Root / "flush_all"  as user=> for {
      currentState <- ctx.state.get
      nodeId       = ctx.config.nodeId
      storagePath  = ctx.config.storagePath
      cache        = currentState.cacheX
      userId       = user.id
      bucketId     = user.bucketName
      elements     <- cache.getAll
      basePath     = Paths.get(s"$storagePath/$nodeId/$userId/$bucketId")
      sinkPath     =(guid:String)=>  basePath.resolve(guid)
      _            <- NIOFiles[IO].createDirectories(basePath)
      _            <- elements.traverse{ element =>
        fs2.Stream.emits(element.item.get.value.bytes).covary[IO].through(Files[IO].writeAll(sinkPath(element.key))).compile.drain
      }
      response     <- Ok("OK!")
    } yield response

  }

}
