import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import fs2.io.file.Files
import mx.cinvestav.commons.fileX.FileMetadata
import org.http4s._
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.implicits._
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci.CIString
import org.typelevel.vault.Vault

import java.io.File
import java.net.URL
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.language.postfixOps
//import org.http4s.client.dsl.io.

class UploadSpec extends munit .CatsEffectSuite {
  val resourceClient =  BlazeClientBuilder[IO](global).resource
  final val TARGET   = "/home/nacho/Programming/Scala/cache-node/target"
  final val SOURCE_FOLDER  = s"$TARGET/source"
  final val SINK_FOLDER  = s"$TARGET/sink"
  final val pdf0File = new File(s"$SOURCE_FOLDER/0.pdf")
  final val pdf0Id   = UUID.fromString("8c729be1-f716-4ec0-b820-e8af649ec173")
  final val pdf1File = new File(s"$SOURCE_FOLDER/1.pdf")
  final val pdf1Id   = UUID.fromString("6434652f-3d07-4eba-ac33-278f04d577e1")
  final val pdf2File = new File(s"$SOURCE_FOLDER/2.pdf")
  final val pdf2Id   = UUID.fromString("2358408e-9d29-4b2e-b9a8-e5590ee0636d")
  final val pdf3File = new File(s"$SOURCE_FOLDER/3.pdf")
  final val pdf3Id   = UUID.fromString("9d60dae7-7527-4bc2-a180-22a2a1740ac7")
//
  final val userId   = UUID.fromString("3acf3090-4025-4516-8fb5-fa672589b465")
  test("Mimetype"){
    val mt = MediaType.forExtension("jpg")
    println(mt)
  }
  test("Upload files"){

    val downloadRequest = (port:Int,id:UUID) => Request[IO](
      method      = Method.GET,
//      uri         = Uri.unsafeFromString(s"http://localhost:4000/download/$id"),
      uri         = Uri.unsafeFromString(s"http://localhost:${port}/download/$id"),
      //      uri         = Uri.unsafeFromString(s"http://localhost:4000/download/${UUID.randomUUID()}"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers     = Headers.empty,
      attributes  = Vault.empty
    )
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("File-Extension"),"pdf"),
          Header.Raw(CIString("Compression-Algorithm"),"lz4"),
        )
      )
    val parts:Vector[Part[IO]] = Vector(
      Part.fileData(name = "pdf0",file = pdf0File,
        headers = Headers(
          Header.Raw(CIString("guid"),pdf0Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf0File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
        )
      ),
      Part.fileData("pdf1",pdf1File,
        headers = Headers(
          Header.Raw(CIString("guid"),pdf1Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf1File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
        )
      ),
      Part.fileData("pdf2",pdf2File,
        headers = Headers(
          Header.Raw(CIString("guid"),pdf2Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf2File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
        )
      ),
      Part.fileData("pdf3",pdf3File,
      headers = Headers(
        Header.Raw(CIString("guid"),pdf3Id.toString),
        Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf3File.toPath).fullname ),
        headers.`Content-Type`(MediaType.application.pdf),
      )
    )
    )
    val multipartOneFile = (part:Part[IO]) => Multipart[IO](
      parts =  Vector(part),

    )
    val pdf0 = multipartOneFile(parts(0))
    val pdf1 = multipartOneFile(parts(1))
    val pdf2 = multipartOneFile(parts(2))
    val pdf3 = multipartOneFile(parts(3))

    val uploadRequest = (nodeUri:Uri,multipart:Multipart[IO],operationId:Int) =>Request[IO](
      method = Method.POST,
      uri = nodeUri,
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers = multipart.headers,
      attributes = Vault.empty
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("Compression-Algorithm"),"lz4"),
          Header.Raw(CIString("Operation-Id"),operationId.toString),
        )
      )


    resourceClient.use{ client => for {
      _                <- IO.println("UPLOADS")
      trace           =NonEmptyList.of[Request[IO]](
//      CACHE_SIZE = 1
        uploadRequest(uri"""http://localhost:4000/upload""",pdf0,1),
        uploadRequest(uri"""http://localhost:4001/upload""",pdf1,2),
        downloadRequest(4000,pdf0Id),
//        downloadRequest(4000,pdf0Id),
//        downloadRequest(4000,pdf0Id),
        downloadRequest(4001,pdf1Id),
        uploadRequest(uri"""http://localhost:4002/upload""",pdf2,3),
        uploadRequest(uri"""http://localhost:4000/upload""",pdf3,3),
//        uploadRequest(uri"""http://localhost:4001/upload""",pdf2,3),
//
//        uploadRequest(uri"""http://localhost:4001/upload""",pdf2),
//        downloadRequest(4001,pdf2Id),
//        downloadRequest(4001,pdf2Id),
//
//        uploadRequest(uri"""http://localhost:4002/upload""",pdf3),
//        downloadRequest(4002,pdf3Id),
//
//        uploadRequest(uri"""http://localhost:4000/upload""",pdf1),
//        downloadRequest(4000,pdf1Id),
      )
      responses <- trace.zipWithIndex.traverse {
        case (x, index) => for {
          _ <- IO.println(s"REQUEST[$index]")
//          res <- client.expect[String](req = x)
          res <- client.stream(x).flatMap{ response=>
            val body = response.body
            val sinkPath = Paths.get(SINK_FOLDER+s"/${UUID.randomUUID()}")
            body.through(Files[IO].writeAll(sinkPath))
          }.compile.drain
//          _ <- IO.println(s"RESPONSE[$index] "+res)
          _ <- IO.sleep(2 seconds)
        } yield res
      }
    //      _ <- IO.println(responses)
//      uploadObjectIds  = NonEmptyList.of[Multipart[IO]](pdf1,pdf2,pdf3)
//      uploadResponse   <- uploadObjectIds.traverse(x=>client.expect[String](req=uploadRequest(x)))
//      _        <- IO.println(s"UPLOAD_RESPONSE $uploadResponse")
//      _ <- IO.sleep(5 seconds)
//      objectIds = NonEmptyList.of[UUID](pdf0Id,pdf0Id,pdf0Id,pdf1Id,pdf2Id,pdf2Id)
//      downloadResponse <-  objectIds.traverse(x=>client.expect[String](req=downloadRequest(x)))
//      _        <- IO.println(s"DOWNLOAD_RESPONSE $downloadResponse")
    } yield ()
    }
  }

  test("Download"){

    val request = Request[IO](
      method      = Method.GET,
      uri         = Uri.unsafeFromString(s"http://localhost:4000/download/$pdf2Id"),
//      uri         = Uri.unsafeFromString(s"http://localhost:4000/download/${UUID.randomUUID()}"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers     = Headers.empty,
      attributes  = Vault.empty
    )
      .putHeaders(
        Headers(Header.Raw(CIString("userId"),userId.toString),
        )
      )


    resourceClient.use{ client => for {
      response <- client.expect[String](req=request)
      _        <- IO.println(s"RESPONSE $response")
    } yield ()
    }
  }

//_____________________
  test("Basics"){
    val rawURL = "http://127.0.1.1:7010/home/nacho/Programming/Scala/data-preparation/target/sink/data/dp-0/e6295492-3cd1-44d1-bccd-fb4f06dd400b/chunks/e6295492-3cd1-44d1-bccd-fb4f06dd400b_0.lz4"
    val url = new URL(rawURL)
    println(url.getHost)
    println(url.getPath)
  }

}
