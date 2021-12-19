package mx.cinvestav.server.controllers
import cats.implicits._
import cats.effect.IO
//
import mx.cinvestav.Declarations.{NodeContextV6, User}
import org.http4s.blaze.client.BlazeClientBuilder
import org.http4s.{AuthedRoutes, Header, Headers, MediaType, Method, Request, Uri}
import org.http4s.{headers=>HEADERS}
import org.http4s.dsl.io._
//.{->, /, NotFound, Ok, POST, Root}
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci.CIString

import scala.concurrent.ExecutionContext.global

object ReplicateController {

  def apply()(implicit ctx:NodeContextV6) = {

    AuthedRoutes.of[User,IO]{
      case authReq@POST -> Root / "replicate" / UUIDVar(guid) as user => for {
        currentState <- ctx.state.get
        cacheX               = currentState.cacheX
        req                  = authReq.req
        headers              = req.headers
        //
        replicationStrategy  = headers.get(CIString("Replication-Strategy")).map(_.head.value).getOrElse("ACTIVE")
        replicationFactor    = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
        replicaNodes         = headers.get(CIString("Replica-Node")).map(x=>x.map(_.value).toList ).getOrElse(List.empty[String])
        _                    <- ctx.logger.debug(s"REPLICATION_STRATEGY $replicationStrategy")
        _                    <- ctx.logger.debug(s"REPLICATION_FACTOR $replicationFactor")
        _                    <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
        //
        getResponse          <- cacheX.get(guid.toString)
        newRes       <- getResponse.item match {
          case Some(value) =>
            if(replicationStrategy == "ACTIVE") for {
              _              <- ctx.logger.debug("ACTIVE_REPLICATION")
              objectX        = value.value
              streamBytes    = fs2.Stream.emits(objectX.bytes).covary[IO]
              objectSize     = objectX.bytes.length
              multipart      = Multipart[IO](parts = Vector(Part[IO](
                headers =  Headers(
                  HEADERS.`Content-Length`(objectSize),
                  HEADERS.`Content-Type`(MediaType.application.pdf),
                  Header.Raw(CIString("guid"),guid.toString),
                  Header.Raw(CIString("filename"),"default")
                ),
                body    =  streamBytes
              )))
              requests      = replicaNodes.map{ url =>
//                urls.map( url =>
                  Request[IO](
                    method  = Method.POST,
                    uri     = Uri.unsafeFromString(url),
                    headers =  multipart.headers
                  )
                    .withEntity(multipart)
                    .putHeaders(
                      Headers(
                        Header.Raw(CIString("User-Id"),user.id.toString),
                        Header.Raw(CIString("Bucket-Id"),user.bucketName)
                      ),
                    )
//                )
              }
              (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
              responses          <- requests.traverse(req=>client.toHttpApp.run(req))
              _                  <- ctx.logger.debug(responses.toString)
              _                  <- finalizer
              res                <- Ok("")
            } yield res
            else for {
              _   <- ctx.logger.debug("PASSIVE_REPLICATION")
              res <- Ok("")
            } yield res
          case None => NotFound()
        }
        response <- Ok("REPLICATE")
      } yield response
    }
  }

}
