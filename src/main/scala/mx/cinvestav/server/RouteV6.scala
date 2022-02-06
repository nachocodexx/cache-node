package mx.cinvestav.server
// Cats
import cats.implicits._
import cats.effect._
import cats.effect.std.Semaphore
import mx.cinvestav.server.controllers.{DownloadController, ReplicateController, UploadController}
// Local
import mx.cinvestav.Declarations.{User,NodeContext}
// Http4s
import org.http4s.AuthedRoutes
import org.http4s.implicits._

object RouteV6 {




  def apply(dSemaphore:Semaphore[IO])(implicit ctx:NodeContext): AuthedRoutes[User, IO] =
    UploadController(dSemaphore) <+> DownloadController(dSemaphore)
//  <+> ReplicateController()
//  <+> FlushAllController()

}
