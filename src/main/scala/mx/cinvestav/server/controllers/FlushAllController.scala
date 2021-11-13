package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect.IO
//
import fs2.io.file.Files
//
import mx.cinvestav.Declarations.{NodeContextV6, User}
import org.http4s.AuthedRoutes
import org.http4s.dsl.io._
//
import cats.nio.file.{Files=>NIOFiles}
import java.nio.file.Paths

object FlushAllController {


  def apply()(implicit ctx:NodeContextV6) = {
    AuthedRoutes.of[User,IO] {

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

}
