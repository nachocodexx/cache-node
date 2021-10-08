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
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import org.typelevel.vault.Vault

import java.io.File
import java.net.URL
import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.ExecutionContext.global
import scala.concurrent.duration._
import scala.language.postfixOps
//import org.http4s.client.dsl.io.

class UploadV6Spec extends munit .CatsEffectSuite {
  trait OperationType
  case object Upload extends OperationType{
    override def toString():String = "UPLOAD"
  }
  case object Download extends OperationType{

    override def toString():String = "DOWNLOAD"
  }
  case class RequestX(
                       operationType:OperationType,
                       arrivalTime:Long,
                       req:Request[IO]
                     )

  val resourceClient =  BlazeClientBuilder[IO](global).resource
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
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
  final val video0 = new File(s"$SOURCE_FOLDER/6.mkv")
  final val video0Id   = UUID.fromString("c5d9d60b-ebde-4ae0-be32-40f5513af0a2")
//
  final val userId   = UUID.fromString("3acf3090-4025-4516-8fb5-fa672589b465")
  test("Mimetype"){
    val mt = MediaType.forExtension("jpg")
    println(mt)
  }
  test("Workload"){
//

    val replicationRequest = (port:Int,id:UUID) => Request[IO](
      method      = Method.POST,
      uri         = Uri.unsafeFromString(s"http://localhost:${port}/api/v6/replicate/$id"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers     = Headers.empty,
      attributes  = Vault.empty
    )
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("Replication-Strategy"),"ACTIVE"),
          Header.Raw(CIString("Replication-Factor"),"2"),
          Header.Raw(CIString("Replica-Node"),"http://localhost:4002/api/v6/upload"),
          Header.Raw(CIString("Replica-Node"),"http://localhost:4001/api/v6/upload"),
        )
      )
    val downloadRequest = (port:Int,id:UUID) => Request[IO](
      method      = Method.GET,
      uri         = Uri.unsafeFromString(s"http://localhost:${port}/api/v6/download/$id"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers     = Headers.empty,
      attributes  = Vault.empty
    )
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
        )
      )
//
    val parts:Vector[Part[IO]] = Vector(
      Part.fileData(name = "pdf0",file = pdf0File,
        headers = Headers(
          Header.Raw(CIString("guid"),pdf0Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf0File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf0File.length())
        )
      ),
      Part.fileData("pdf1",pdf1File,
        headers = Headers(
          Header.Raw(CIString("guid"),pdf1Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf1File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf1File.length())
        )
      ),
      Part.fileData("pdf2",pdf2File,
        headers = Headers(
          Header.Raw(CIString("guid"),pdf2Id.toString),
          Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf2File.toPath).fullname ),
          headers.`Content-Type`(MediaType.application.pdf),
          headers.`Content-Length`(pdf2File.length())
        )
      ),
      Part.fileData("pdf3",pdf3File,
      headers = Headers(
        Header.Raw(CIString("guid"),pdf3Id.toString),
        Header.Raw(CIString("filename"), FileMetadata.fromPath(pdf3File.toPath).fullname ),
        headers.`Content-Type`(MediaType.application.pdf),
        headers.`Content-Length`(pdf3File.length())
      )
      ),
      Part.fileData("video0",video0,
          headers = Headers(
            Header.Raw(CIString("guid"),video0Id.toString),
            Header.Raw(CIString("filename"), FileMetadata.fromPath(video0.toPath).fullname ),
            headers.`Content-Type`(MediaType.video.mpv),
            headers.`Content-Length`(video0.length())
          )
    )
    )
    val multipartOneFile = (part:Part[IO]) => Multipart[IO](
      parts =  Vector(part),

    )
//
    val pdf0Multipart = multipartOneFile(parts(0))
    val pdf1Multipart = multipartOneFile(parts(1))
    val pdf2Multipart = multipartOneFile(parts(2))
    val pdf3Multipart = multipartOneFile(parts(3))
    val video0Multipart = multipartOneFile(parts(4))

    val uploadRequest = (port:Int,multipart:Multipart[IO],operationId:Int) =>Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(s"http://localhost:$port/api/v6/upload"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers = multipart.headers,
      attributes = Vault.empty
    )
      .withEntity(multipart)
      .putHeaders(
        Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("Operation-Id"),operationId.toString),
        )
      )
    val flushAllRequest = (port:Int,operationId:Int) =>Request[IO](
      method      = Method.POST,
      uri         = Uri.unsafeFromString(s"http://localhost:$port/api/v6/flush_all"),
      httpVersion = HttpVersion.`HTTP/1.1`,
      headers     = Headers(
          Header.Raw(CIString("User-Id"),userId.toString),
          Header.Raw(CIString("Bucket-Id"),"nacho-bucket"),
          Header.Raw(CIString("Operation-Id"),operationId.toString),
      ),
      attributes = Vault.empty
    )

    var lastArrivalTime = Long.MinValue
    resourceClient.use{ client => for {
      _      <- IO.unit
      trace  = NonEmptyList.of[RequestX](
//        RequestX(Upload,500,uploadRequest(4000,pdf0Multipart,0)),
//        RequestX(Upload,500,replicationRequest(4000,pdf0Id) ),
//        RequestX(Upload,1000,uploadRequest(4000,pdf1Multipart,0)),
//        RequestX(Upload,1000,uploadRequest(4000,video0Multipart,1)),
//      ____________________________________________________________
        RequestX(Download,1000,downloadRequest(4001,pdf0Id)),
        RequestX(Download,1500,downloadRequest(4002,pdf0Id)),
//       ____________________________________________________________
//        RequestX(Upload,1000,uploadRequest(4000,pdf2Multipart,0)),
//          RequestX(Upload,2000,flushAllRequest(4000,0)),
//        RequestX(Upload,1700,downloadRequest(4000,pdf0Id)),
//        RequestX(Upload,1900,downloadRequest(4000,pdf1Id)),
//        RequestX(Upload,1000,downloadRequest(4000,pdf1Id)),
//        RequestX(Upload,1000,downloadRequest(4000,pdf1Id)),
//        RequestX(Upload,1500,downloadRequest(4000,pdf0Id)),
//        RequestX(Upload,1500,downloadRequest(4000,pdf0Id)),
//        RequestX(Upload,1500,downloadRequest(4000,pdf0Id)),
//      _____________________________________________________________
      )

      responses <- trace.zipWithIndex.traverse {
        case (reqx, index) => for {
          _            <- IO.unit
          waitingTime_ = reqx.arrivalTime - lastArrivalTime
          _            <- IO.println(waitingTime_)
          waitingTime  = if(waitingTime_ < 0 )  0 else waitingTime_
          _            <- IO.sleep(waitingTime milliseconds)
          resultId     = UUID.randomUUID()
          _         <- IO.println(s"${reqx.operationType.toString} - $resultId")
          res       <- client.stream(reqx.req).flatMap{ response=>
            val body = response.body
            if(reqx.operationType == Download){
              val sinkPath = Paths.get(SINK_FOLDER+s"/$resultId.pdf")
              body.through(Files[IO].writeAll(sinkPath))
            }
            else fs2.Stream.eval(IO.unit)
          }.compile.drain
          _ <- IO.delay{ lastArrivalTime = waitingTime }
        } yield res
      }
    } yield ()
    }


  }


}
