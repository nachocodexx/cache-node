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
import retry._
import retry.implicits._
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
      val digest       = headers.get(CIString("Digest")).map(_.head.value).getOrElse("DIGEST")
      val path         = Paths.get(s"${ctx.config.storagePath}/$objectId")
//    __________________________________________________________________________________________________________________
      val program = for {
        _            <- s.acquire
        arrivalTime  <- IO.monotonic.map(_.toNanos)
        currentState <- ctx.state.get
        balls        = currentState.balls
        maybeBall    = balls.find(_.id == objectId)
        waitingTime  <- IO.monotonic.map(_.toNanos - arrivalTime)
        appIO        = for {
          multipart    <- req.as[Multipart[IO]]
          response     <- if(path.toFile.exists()) Forbidden(s"Ball $objectId already exists")
          else {
            for {
              _ <- IO.unit
              ball          = Ball(id = objectId,size = objectSize,metadata =Map("PATH"->path.toString))
              objectIds     <- multipart.parts.traverse{ part =>
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
              _             <- ctx.config.pool.uploadCompletedv2(objectId = objectId,operationId= operationId).start
              _             <- ctx.logger.info(s"UPLOAD_PUSH $operationId $objectId $objectSize $serviceTime $waitingTime")
              response      <- NoContent()
            } yield response
          }
        } yield response



        response <- appIO.handleErrorWith{ e=>
          for {
            _            <- ctx.logger.error(e.getMessage)
            exists       = path.toFile.exists()
            _            <- ctx.logger.debug(s"PULL_DATA $objectUri")
            _            <- ctx.logger.debug(s"PATH_EXISTS $exists")
            _            <- ctx.logger.debug(s"MAYBE_BALL ${maybeBall.isDefined}")
//          _____________________________________________________________________________
            response     <- if(exists && maybeBall.isDefined) Forbidden(
              s"Ball $objectId already exists",
              Headers(
                Header.Raw(CIString("Error-Message"),s"Ball $objectId already exists"),
                Header.Raw(CIString("Error-Code"),"0"),
              )
            )
            else {
              for {
                _             <- IO.unit
                ball          = Ball(
                  id = objectId,
                  size = objectSize,
                  metadata =Map("PATH"->path.toString,"DIGEST"->digest)
                )
                _             <- downloadFromURI(objectId = objectId, objectUri = objectUri).compile.drain.onError{ e=>
                    ctx.logger.error(e.getMessage)
                  }
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
                _             <- ctx.config.pool.uploadCompletedv2(objectId = objectId, operationId = operationId )
//                  .retryingOnFailures(
//                    wasSuccessful = s=>(s.code == 204).pure[IO],
//                    policy        = RetryPolicies.limitRetries[IO](20) join RetryPolicies.exponentialBackoff[IO](100 milliseconds),
//                    onFailure     = (s,rd)=> ctx.logger.error(s"ON_FAILURE $s")
//                  )
                  .start
                _             <- ctx.logger.info(s"UPLOAD_PULL $operationId $objectId $objectSize $serviceTime $waitingTime")
                response      <- NoContent()
          } yield response
        }

          } yield response
         }
        _ <- ctx.logger.debug(s"UPLOAD_RESPONSE $response")
        _ <- s.release
        _                  <- ctx.logger.debug("_____________________________________________")
      } yield response

      program.onError{ e=>
        ctx.logger.error(e.getMessage)
      }

  }

}
