package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import fs2.Stream
import fs2.io.file.Files
import org.http4s._
import org.http4s.dsl.io._
import mx.cinvestav.Declarations.{Ball, NodeContext}
import mx.cinvestav.commons.types.{Operation, UploadCompleted}
import org.http4s.multipart.Multipart
import org.typelevel.ci.CIString

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration._
import language.postfixOps

object UploadV3 {



  //__________________________________________________________
  def downloadFromURI(objectId:String,objectUri:String)(implicit ctx:NodeContext) = {
    val downloadReq = Request[IO](
      method = Method.GET,
      uri = Uri.unsafeFromString(objectUri )
    )
    ctx.client.stream(downloadReq).flatMap{
      res=>
        val path = Paths.get(s"${ctx.config.storagePath}/$objectId")
        val s = res.body.through(Files[IO].writeAll(
          path = path,
//          flags = List
        ))
        if(path.toFile.exists()) Stream.empty else s
    }
  }
//__________________________________________________________


  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root / "upload" =>
      val headers      = req.headers
      val objectId     = headers.get(CIString("Object-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
      val objectUri    = headers.get(CIString("Object-Uri")).map(_.head.value).getOrElse("")
      val operationId  = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
      val objectSize   = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
      val serialNumber = headers.get(CIString("Serial-Number")).flatMap(_.head.value.toIntOption).getOrElse(0)


      val path         = Paths.get(s"${ctx.config.storagePath}/$objectId")
//    __________________________________________________________________________________________________________________
      for {
        arrivalTime  <- IO.monotonic.map(_.toNanos)
        _            <- s.acquire
        waitingTime  <- IO.monotonic.map(_.toNanos - arrivalTime)
        appIO          = for {
          multipart    <- req.as[Multipart[IO]]
          response     <- if(path.toFile.exists()) Forbidden(s"Ball $objectId already exists")
          else {
            for {
              _ <- IO.unit
              ball         = Ball(id = objectId,size = objectSize,metadata =Map("PATH"->path.toString))
              objectIds    <- multipart.parts.traverse{ part =>
                val name    = part.name.getOrElse(s"${UUID.randomUUID().toString}")
                val body    = part.body
                body.through(Files[IO].writeAll(path = path)).compile.drain *> name.pure[IO]
              }
              _             <- IO.sleep(ctx.config.delayReplicaMs milliseconds)
              _             <- ctx.state.update{ s=>
                s.copy(balls = s.balls :+ ball)
              }
              departureTime <- IO.monotonic.map(_.toNanos)
              serviceTime   = departureTime - arrivalTime
              completedUp   = UploadCompleted(operationId =operationId,
                serialNumber = 0,
                arrivalTime = arrivalTime,
                serviceTime = serviceTime,
                waitingTime =waitingTime,
                idleTime = 0L,
                objectId = objectId,
                objectSize = objectSize,
                nodeId = ctx.config.nodeId
              )
              _             <- ctx.state.update{s=>s.copy(completedOperations = s.completedOperations:+ completedUp)}
              _             <- ctx.config.pool.uploadCompletedv2(objectIds = objectIds.toList,operationId= operationId).start
              _ <- ctx.logger.debug(s"UPLOAD_M $objectId $serviceTime")
              response      <- NoContent()
            } yield response
          }
        } yield response



        response <- appIO .handleErrorWith{ e=>
          for {
            _            <- ctx.logger.error(e.getMessage)
            _            <- ctx.logger.debug(s"PULL_DATA $objectUri")
            response     <- if(path.toFile.exists()) Forbidden(s"Ball $objectId already exists")
            else {
              for {
                _             <- IO.unit
                ball          = Ball(id = objectId,size = objectSize,metadata =Map("PATH"->path.toString))
                _             <- downloadFromURI(objectId = objectId, objectUri = objectUri).compile.drain
                _             <- IO.sleep(ctx.config.delayReplicaMs milliseconds)
                _             <- ctx.state.update{ s=>s.copy(balls = s.balls :+ ball)}
                departureTime <- IO.monotonic.map(_.toNanos)
                serviceTime   = departureTime - arrivalTime
                completedUp   = UploadCompleted(operationId =operationId,
                  serialNumber = 0,
                  arrivalTime = arrivalTime,
                  serviceTime = serviceTime,
                  waitingTime =waitingTime,
                  idleTime = 0L,
                  objectId = objectId,
                  objectSize = objectSize,
                  nodeId = ctx.config.nodeId
                )
                _             <- ctx.state.update{s=>s.copy(completedOperations = s.completedOperations:+ completedUp)}
                _             <- ctx.config.pool.uploadCompletedv2(objectIds = List(objectId),operationId = operationId ).start
                _ <- ctx.logger.debug(s"UPLOAD_D $objectId $serviceTime")
                response      <- NoContent()
          } yield response
        }

          } yield response
      }
        _ <- s.release
      } yield response

  }

}
