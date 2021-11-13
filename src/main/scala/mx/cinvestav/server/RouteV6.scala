package mx.cinvestav.server
// Cats
import cats.implicits._
import cats.effect._
import mx.cinvestav.server.controllers.{DownloadController, FlushAllController, ReplicateController, UploadController}
// Local
import mx.cinvestav.Declarations.{User,NodeContextV6}
// Http4s
import org.http4s.AuthedRoutes
import org.http4s.implicits._

object RouteV6 {




  def apply()(implicit ctx:NodeContextV6): AuthedRoutes[User, IO] =
    UploadController() <+> DownloadController() <+> ReplicateController() <+> FlushAllController()

}
