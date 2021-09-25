package mx.cinvestav.server

import cats.data.EitherT
import cats.effect._
import fs2.io.file.Files
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.Declarations.DownloadError
import mx.cinvestav.server.Routes.DownloadPayload
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.{Header, Headers, Method, Request, Uri}
import org.typelevel.ci.CIString

import java.nio.file.Path
import scala.concurrent.ExecutionContext.global

object Client {
  def downloadFileE(url:String,sourcePath:String,destination:Path): EitherT[IO, DownloadError, Unit] = {
    EitherT(
    downloadFile(
      url=url,
      sourcePath = sourcePath,
      destination = destination
    )
      .map(x=>Right(()))
      .handleError(e=>Left(e.getMessage))
    )
      .leftMap(DownloadError)
  }


  def downloadFileV3(userId:String,bucketName:String,url:String,destination:Path):IO[Unit] = {
    BlazeClientBuilder[IO](global).resource.use{ client =>
      val request = Request[IO](
        method = Method.GET,
        uri = Uri.unsafeFromString(url)
      ).putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId),
          Header.Raw(CIString("Bucket-Id"),bucketName),
          Header.Raw(CIString("Compression-Algorithm"),"lz4")
        )
      )
      client.stream(request).flatMap{ response =>
        response.body.through(Files[IO].writeAll(destination)  )
      }.compile.drain
    }
  }
  def downloadFilev2(url:String,destination:Path):IO[Unit] = {
    BlazeClientBuilder[IO](global).resource.use{ client =>
      val request = Request[IO](
        method = Method.GET,
        uri = Uri.unsafeFromString(url)
      )
      client.stream(request).flatMap{ response =>
        response.body.through(Files[IO].writeAll(destination)  )
      }.compile.drain
    }
  }

  def downloadFile(url:String,sourcePath:String,destination:Path): IO[Unit] = {
    BlazeClientBuilder[IO](global).resource.use{ client =>
      val payload= DownloadPayload(source = sourcePath)
      val request = Request[IO](
        method = Method.POST,
        uri = Uri.unsafeFromString(url),
      ).withEntity(payload.asJson.noSpaces)

      client.stream(request).flatMap{ response =>
        response.body.through(Files[IO].writeAll(destination))
      }.compile.drain
    }
  }
}
