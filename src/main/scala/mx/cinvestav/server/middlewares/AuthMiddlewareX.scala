package mx.cinvestav.server.middlewares
import cats.implicits._
import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import mx.cinvestav.Declarations.{NodeContextV6, User}
import org.http4s.Request
import org.http4s.server.AuthMiddleware
import org.typelevel.ci.CIStringSyntax

import java.util.UUID

object AuthMiddlewareX {


  def authUser()(implicit ctx:NodeContextV6):Kleisli[OptionT[IO,*],Request[IO],User] =
    Kleisli{ req=> for {
      _          <- OptionT.liftF(IO.unit)
      headers    = req.headers
      //      _          <- OptionT.liftF(ctx.logger.debug(headers.headers.mkString(" // ")))
      maybeUserId     = headers.get(ci"User-Id").map(_.head).map(_.value)
      maybeBucketName = headers.get(ci"Bucket-Id").map(_.head).map(_.value)
      //      _          <- OptionT.liftF(ctx.logger.debug(maybeUserId.toString+"//"+maybeBucketName.toString))
      ress            <- (maybeUserId,maybeBucketName) match {
        case (Some(userId),Some(bucketName)) =>   for {
          x  <- OptionT.liftF(User(id = UUID.fromString(userId),bucketName=bucketName  ).pure[IO])
          //          _  <- OptionT.liftF(ctx.logger.debug("AUTHORIZED"))
        } yield x
        case (Some(_),None) => OptionT.liftF(User(id = UUID.fromString("b952f36e-e2f4-4b5a-93f1-e6d3af1b9c75"),bucketName="DEFAULT"  ).pure[IO])
        //          OptionT.none[IO,User]
        case (None,Some(_)) => OptionT.liftF(User(id = UUID.fromString("b952f36e-e2f4-4b5a-93f1-e6d3af1b9c75"),bucketName="DEFAULT"  ).pure[IO])
        //          OptionT.none[IO,User]
        case (None,None )   => OptionT.liftF(User(id = UUID.fromString("b952f36e-e2f4-4b5a-93f1-e6d3af1b9c75"),bucketName="DEFAULT"  ).pure[IO])
        //          OptionT.none[IO,User]
      }

    } yield ress
    }

//  def authMiddleware(implicit ctx:NodeContextV6):AuthMiddleware[IO,User] =
  def apply(implicit ctx:NodeContextV6): AuthMiddleware[IO, User] =
    AuthMiddleware(authUser=authUser)

}
