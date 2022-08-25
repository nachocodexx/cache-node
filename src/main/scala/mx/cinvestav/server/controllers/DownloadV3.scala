package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import fs2.io.file.Files
import org.http4s._
import org.http4s.dsl.io._
import mx.cinvestav.Declarations.{Ball, NodeContext}
import org.http4s.multipart.Multipart
import org.typelevel.ci.CIString

import java.nio.file.Paths
import java.util.UUID
import scala.concurrent.duration._
import language.postfixOps
import fs2.io.file
import mx.cinvestav.commons.types.{DownloadCompleted, UploadCompleted}
import retry._
import retry.implicits._

object DownloadV3 {

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@GET -> Root / "download" / ballId =>
      val getTime      = IO.monotonic.map(_.toNanos)
      val program = for{
        arrivalTime   <- getTime
        _             <- s.acquire
        waitingTime   <- getTime.map(_ - arrivalTime)
        headers       = req.headers
  //    __________________________________________________________________________________________________________________
        operationId    = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
        objectSize     = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
        currentState   <- ctx.state.get
        balls          = currentState.balls
        maybeBall      = balls.find(_.id == ballId)
        maybeOperation = currentState.completedOperations.find(_.operationId == operationId)
        response       <- if(maybeOperation.isDefined)
          Forbidden(s"$operationId was already performed.",
            Headers(
              Header.Raw(CIString("Error-Message"),"$operationId was already performed."),
              Header.Raw(CIString("Error-Code"),"0"),
            )
          )
        else {
           maybeBall match {
            case Some(ball) =>
              val path   = Paths.get(s"${ctx.config.storagePath}/$ballId")
              val stream = file.Files[IO].readAll(path = path, chunkSize = 8192)
              if(operationId.isEmpty) Ok(stream)
              else {
                for{
                  _             <- ctx.config.pool.downloadCompletedv2(objectId = ballId,operationId = operationId).start
                  departureTime <- getTime
                  serviceTime   = departureTime - arrivalTime
                  cUp           = DownloadCompleted(
                    operationId =operationId,
                    serialNumber = 0,
                    arrivalTime = arrivalTime,
                    serviceTime = serviceTime,
                    waitingTime = waitingTime,
                    idleTime = 0L,
                    objectId = ballId,
                    objectSize = objectSize, nodeId = ctx.config.nodeId
                  )
                  _             <- ctx.state.update{s=>s.copy(
                    completedOperations = s.completedOperations :+ cUp
                  )}
                  response      <- Ok(stream)
                  _             <- ctx.logger.info(s"DOWNLOAD $operationId $ballId $objectSize $serviceTime $waitingTime")
                  _             <- ctx.logger.debug("_____________________________________________")
                } yield  response
              }
            case None => NotFound()
          }
        }
        _             <- s.release
    } yield response
      program.onError{ e=>
        ctx.logger.error(e.getMessage)
      }
  }

}
