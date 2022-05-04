package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect.IO
import fs2.Stream
import fs2.io.file.Files
import mx.cinvestav.Declarations.{NodeContext, ObjectD, ObjectS}
import mx.cinvestav.commons.types.ObjectMetadata
import mx.cinvestav.events.Events
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.multipart.{Multipart, Part}
import org.typelevel.ci.CIString
import org.http4s.{headers=>HEADERS}
import java.util.UUID

object ActiveReplication {


  def apply()(implicit ctx:NodeContext) = HttpRoutes.of[IO]{
    case req@POST -> Root / "replicate" / "active" / objectId =>
      for {
      _              <- ctx.logger.debug(s"ACTIVE_REPLICATION $objectId")
      requestStartAt <- IO.monotonic.map(_.toNanos)
      currentState   <- ctx.state.get
      headers        = req.headers
      replicaNodes   = headers.get(CIString("Replica-Node")).map(_.map(_.value).toList).getOrElse(Nil)
      operationIds   = headers.get(CIString("Operation-Id")).map(_.map(_.value).toList).getOrElse(Nil)
//      _ <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
//      _ <- ctx.logger.debug(s"OPERATION_IDS $operationIds")
      events         = Events.relativeInterpretEventsMonotonic(currentState.events)
      maybeObject    <- Events.getObjectIds(events = events).find(_ == objectId)
        .traverse(currentState.cache.lookup)
        .map(_.flatten)
      _ <- ctx.logger.debug(maybeObject.toString)

      res <- maybeObject match {
        case Some(currentObject) => for {
//          _                   <- ctx.logger.debug(s"REPLICATE_OBJECT ${currentObject.guid}")
          arrivalTime         <- IO.realTime.map(_.toNanos)
          (sBytes,objectSize) = currentObject match {
            case o@ObjectD(guid, path, metadata)  =>
              (Files[IO].readAll(path,chunkSize=8192),metadata.get("objectSize").flatMap(_.toLongOption).getOrElse(0L))
            case o@ObjectS(guid, bytes, metadata) =>
              (Stream.emits(bytes),metadata.get("objectSize").flatMap(_.toLongOption).getOrElse(0L))
          }
          uploadRequests      = (replicaNodes zip operationIds) .map{
            case (replicaNode,operationId) =>
              val oHeaders = Headers(
                Header.Raw(CIString("Object-Id"),currentObject.guid),
                HEADERS.`Content-Type`(MediaType.text.plain),
                HEADERS.`Content-Length`(objectSize)
              )
              val metadata = currentObject.metadata
              val oMetadata = ObjectMetadata.fromMap(metadata)

              val multipart = Multipart[IO](
                parts = Vector(
                  Part(headers = oHeaders,body = sBytes)
                )
              )
//              val _operationId = s"${operationId}_${oMetadata.blockIndex}"

              val objectHeaders = Headers(
                Header.Raw(CIString("Operation-Id"),operationId),
                Header.Raw(CIString("User-Id"),"SYSTEM"),
                Header.Raw(CIString("Bucket-Id"), "bucket-0" ),
                Header.Raw(CIString("Object-Id"),objectId),
                Header.Raw(CIString("Object-Size"), objectSize.toString ),
                Header.Raw(CIString("File-Path"),oMetadata.filePath),
                Header.Raw(CIString("File-Extension"),oMetadata.fileExtension),
                Header.Raw(CIString("Digest"),oMetadata.digest),
                Header.Raw(CIString("Request-Start-At"),requestStartAt.toString),
                Header.Raw(CIString("Catalog-Id"),oMetadata.catalogId),
                Header.Raw(CIString("Block-Index"),oMetadata.blockIndex.toString),
                Header.Raw(CIString("Block-Total"),oMetadata.blockTotal.toString),
                Header.Raw(CIString("Compression-Algorithm"),oMetadata.compressionAlgorithm),
                Header.Raw(CIString("Impact-Factor"),"0.0"),
                Header.Raw(CIString("Arrival-Time"),arrivalTime.toString),
                Header.Raw(CIString("Replication-Factor"),"1"),
              )

            for {
              _ <- ctx.logger.debug(oMetadata.toString)
              _ <- ctx.logger.debug(s"REPLICA_NODE_ID $replicaNode")
              _ <- ctx.logger.debug(s"OPERATION_ID $operationId")
             request = Request[IO](
                 method = Method.POST,
                 uri    = Uri.unsafeFromString(s"http://$replicaNode:6666/api/v2/upload"),
                 headers = multipart.headers
               ).withEntity(multipart).putHeaders(objectHeaders)
            } yield request
          }
          responses <- uploadRequests.traverse{request =>
            ctx.client.status(request)
          }
          _ <- ctx.logger.debug(s"RESPONSES $responses")
          res <- NoContent()
        } yield res
        case None => NotFound()
      }

      _ <- ctx.logger.debug(res.toString)
    } yield res


    case req@POST -> Root / "replicate" / "passive" / objectId => for {
        _              <- ctx.logger.debug(s"PASSIVE_REPLICATION $objectId")
        requestStartAt <- IO.monotonic.map(_.toNanos)
        currentState   <- ctx.state.get
        headers        = req.headers
        replicaNodes   = headers.get(CIString("Replica-Node")).map(_.map(_.value).toList).getOrElse(Nil)
        operationIds   = headers.get(CIString("Operation-Id")).map(_.map(_.value).toList).getOrElse(Nil)
        events         = Events.relativeInterpretEventsMonotonic(currentState.events)
        maybeObject    <- Events.getObjectIds(events = events).find(_ == objectId)
          .traverse(currentState.cache.lookup)
          .map(_.flatten)
        _ <- ctx.logger.debug(maybeObject.toString)

        res <- maybeObject match {
          case Some(currentObject) => for {
            _                   <- ctx.logger.debug(s"REPLICATE_OBJECT ${currentObject.guid}")
            arrivalTime         <- IO.realTime.map(_.toNanos)
            (sBytes,objectSize) = currentObject match {
              case o@ObjectD(guid, path, metadata)  =>
                (Files[IO].readAll(path,chunkSize=8192),metadata.get("objectSize").flatMap(_.toLongOption).getOrElse(0L))
              case o@ObjectS(guid, bytes, metadata) =>
                (Stream.emits(bytes),metadata.get("objectSize").flatMap(_.toLongOption).getOrElse(0L))
            }
            uploadRequests      = (replicaNodes zip operationIds) .map{
              case (replicaNode,operationId) =>
                val oHeaders = Headers(
                  Header.Raw(CIString("Object-Id"),currentObject.guid),
                  HEADERS.`Content-Type`(MediaType.text.plain),
                  HEADERS.`Content-Length`(objectSize)
                )
                val metadata = currentObject.metadata
                val oMetadata = ObjectMetadata.fromMap(metadata)

                val multipart = Multipart[IO](
                  parts = Vector(
                    Part(headers = oHeaders,body = sBytes)
                  )
                )
//                val _operationId = s"${operationId}_${oMetadata.blockIndex}"

                val objectHeaders = Headers(
                  Header.Raw(CIString("Operation-Id"),operationId),
                  Header.Raw(CIString("User-Id"),"SYSTEM"),
                  Header.Raw(CIString("Bucket-Id"), "bucket-0" ),
                  Header.Raw(CIString("Object-Id"),objectId),
                  Header.Raw(CIString("Object-Size"), objectSize.toString ),
                  Header.Raw(CIString("File-Path"),oMetadata.filePath),
                  Header.Raw(CIString("File-Extension"),oMetadata.fileExtension),
                  Header.Raw(CIString("Digest"),oMetadata.digest),
                  Header.Raw(CIString("Request-Start-At"),requestStartAt.toString),
                  Header.Raw(CIString("Catalog-Id"),oMetadata.catalogId),
                  Header.Raw(CIString("Block-Index"),oMetadata.blockIndex.toString),
                  Header.Raw(CIString("Block-Total"),oMetadata.blockTotal.toString),
                  Header.Raw(CIString("Compression-Algorithm"),oMetadata.compressionAlgorithm),
                  Header.Raw(CIString("Impact-Factor"),"0.0"),
                  Header.Raw(CIString("Arrival-Time"),arrivalTime.toString),
                  Header.Raw(CIString("Replication-Factor"),"1"),
                )

                for {
                  _ <- ctx.logger.debug(oMetadata.toString)
                  _ <- ctx.logger.debug(s"REPLICA_NODE_ID $replicaNode")
                  _ <- ctx.logger.debug(s"OPERATION_ID $operationId")
                  request = Request[IO](
                    method = Method.POST,
                    uri    = Uri.unsafeFromString(s"http://$replicaNode:6666/api/v2/upload"),
                    headers = multipart.headers
                  ).withEntity(multipart).putHeaders(objectHeaders)
                } yield request
            }
            responses <- uploadRequests.traverse{request =>
              ctx.client.status(request)
            }
            res <- NoContent()
          } yield res
          case None => NotFound()
        }

        _ <- ctx.logger.debug(res.toString)
      } yield res
  }
}
