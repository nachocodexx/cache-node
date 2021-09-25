package mx.cinvestav.server

import cats.implicits._
import cats.effect._
import fs2.io.file.Files
import mx.cinvestav.Declarations.NodeContextV5
import mx.cinvestav.commons.compression
import io.circe._
import io.circe.generic.auto._
import mx.cinvestav.cache.cache.CachePolicy
import mx.cinvestav.commons.fileX.FileMetadata
import mx.cinvestav.commons.types.ObjectMetadata
import org.http4s._
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.multipart.Multipart
import org.http4s.headers._
import org.typelevel.ci.{CIString, CIStringSyntax}

import java.nio.file.Paths
import java.util.UUID

object Routes {
//  case class DownloadPayload(id:String, extension:String)
  case class DownloadPayload(source:String)
  case class UploadResponse(operationId:String,uploadSize:Long,duration:Long)


  def hello(implicit ctx:NodeContextV5): HttpRoutes[IO] = HttpRoutes.of[IO]{
    case req@GET -> Root   =>
      Ok(s"Hello I'm ${ctx.config.nodeId}")
    case req@GET -> Root /"download"/ UUIDVar(guid) =>for {
      timestamp        <- IO.realTime.map(_.toMillis)
      currentState     <- ctx.state.get
      cachePolicy      = ctx.config.cachePolicy
//      downloadCounter  = currentState.downloadCounter
      cache            = currentState.cache
      cacheX           = CachePolicy(cachePolicy)
      key              = guid.toString
      readRes          <- cacheX.read(cache,key)
      _                <- ctx.state.update(_.copy(cache = readRes.newCache))
//      _ <- ctx.logger.debug(s"IS_HIT $isHit")

      response     <- Ok("Download")
    } yield response
    case req@POST -> Root / "upload" => for {
      timestamp            <- IO.realTime.map(_.toMillis)
      cacheX               = CachePolicy(ctx.config.cachePolicy)
      payload              <- req.as[Multipart[IO]]
      parts                = payload.parts
      headers              = req.headers
      compressionAlgorithm = headers.headers.find(_.name===`Accept-Encoding`.headerInstance.name).map(_.value.toUpperCase).getOrElse(compression.LZ4)
      userId               = headers.get(ci"userId").map(_.head).map(_.value)
      bucketName           = headers.get(ci"bucketName").map(_.head).map(_.value)
      _ <- ctx.logger.debug(userId.toString)
//      _            <- ctx.logger.debug(s"COMPRESSION $compression")
      _            <- headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
      _            <- parts.traverse{ part=>
         for {
           currentState     <- ctx.state.get
           cacheSize        = currentState.cacheSize
           cache            = currentState.cache
//           _                <- ctx.logger.debug(s"PART $part")
           _                <- part.headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
           partHeaders      = part.headers
//         Metadata
           guid             = partHeaders.get(key =ci"guid").map(_.head).map(_.value).map(UUID.fromString).getOrElse(UUID.randomUUID())
           originalFilename = part.filename
           originalName     = part.name
           checksum         = ""
           ca               = compression.fromString(compressionAlgorithm)
           metadata         = ObjectMetadata(
             guid =  guid,
             size  = 0L,
             compression = ca.token,
             bucketName = "default",
             filename = originalFilename.getOrElse("default"),
             name = originalName.getOrElse("default"),
             locations = Nil,
             policies = Nil,
             checksum = "",
             userId = userId.getOrElse(UUID.randomUUID().toString),
             version = 1,
             timestamp = timestamp,
             contentType = "",
             extension = ""
           )
//
           stream           = part.body
//
           _                <- ctx.logger.debug("GUID "+guid.toString)
           putRes           <- cacheX.put(cache,guid.toString)
           _                <- putRes.evicted match {
             case Some(value) =>  for {
               _ <- ctx.logger.info(s"EVICTION ${value.key}")
               _ <- ctx.logger.debug("MIGRATION")
             } yield ()
             case None =>
               IO.unit
           }
//           _                <- ctx.logger.info(s"EVICTED ${putRes.evicted.map(_.key).getOrElse("NONE")}")
           _                <- ctx.state.update(_.copy(cache=putRes.newCache))
         } yield ()
       }
//      _         <- ctx.logger.debug(s"PARTS ${parts.length}")
      //     WRITE TO DISK
      //
      endedAt   <- IO.realTime.map(_.toMillis)
      payload   = UploadResponse(operationId = UUID.randomUUID().toString ,uploadSize = 0L,duration = endedAt-timestamp )
      response  <- Ok("RESPONSE")
    } yield response

    case req@GET ->  "v2" /: filePath => for {
      _          <- IO.unit
      path       = Paths.get(filePath.toAbsolute.toString())
      file       = path.toFile
      _          <- ctx.logger.debug(s"FILE_PATH $path")
      response   <- if(!file.exists())  NotFound()
      else for {
        _        <- ctx.logger.debug(s"FILE_SIZE ${file.length()} bytes")
        metadata = FileMetadata.fromPath(path)
        _        <- ctx.logger.debug(s"FILE_EXTENSION ${metadata.extension}")
        bytes    = Files[IO].readAll(path,8192)
        _        <- ctx.logger.info(s"DOWNLOAD ${metadata.fullname} ${metadata.size.value.get}")
        response <- Ok(bytes)
      } yield response
    } yield response
  }

  def apply()(implicit ctx:NodeContextV5): HttpRoutes[IO] = hello

}
