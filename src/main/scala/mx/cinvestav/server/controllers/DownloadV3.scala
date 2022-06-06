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

object DownloadV3 {

  def apply(s:Semaphore[IO])(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@GET -> Root / "download" / ballId => for{
      _            <- IO.unit
      headers      = req.headers
//    __________________________________________________________________________________________________________________
      operationId  = headers.get(CIString("Operation-Id")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
      objectSize   = headers.get(CIString("Object-Size")).flatMap(_.head.value.toLongOption).getOrElse(0L)
//    __________________________________________________________________________________________________________________
      getTime      = IO.monotonic.map(_.toNanos)
      arrivalTime  <- getTime
      _ <- s.acquire
      waitingTime  <- getTime.map(_ - arrivalTime)
      currentState <- ctx.state.get
      balls        = currentState.balls
      maybeBall    = balls.find(_.id == ballId)
      response     <- maybeBall match {
        case Some(ball) =>
          val path = Paths.get(s"${ctx.config.storagePath}/$ballId")
          val s = file.Files[IO].readAll(path = path, chunkSize = 8192)
          Ok(s)
        case None => NotFound()
      }
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
      _             <- s.release
      _             <- ctx.config.pool.downloadCompletedv2(objectId = ballId,operationId = operationId).start
    } yield response
  }

}
