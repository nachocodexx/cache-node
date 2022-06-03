package mx.cinvestav.server.controllers

import cats.implicits._
import fs2.Stream
import fs2.io.file.Files
import cats.effect.IO
import cats.effect.kernel.Outcome
import cats.effect.std.Semaphore
import mx.cinvestav.Declarations.{IObject, ObjectD, UploadHeadersOps}
import mx.cinvestav.Helpers
import mx.cinvestav.commons.events.{EventXOps, ObjectHashing, PutCompleted}
import mx.cinvestav.commons.types.{ObjectMetadata, ReplicationProcess, UploadHeaders}
import org.http4s.{AuthedRequest, Method, Request, Response, Uri}

import java.nio.file.Paths
//
import mx.cinvestav.Declarations.{NodeContext, ObjectS, User}
import mx.cinvestav.cache.CacheX
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.{Del, Push, Put}
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.PushResponse
import mx.cinvestav.commons.security.SecureX
//
import org.http4s.{headers=>HEADERS}
import org.http4s.{AuthedRoutes, Header, Headers, MediaType}
import org.http4s.dsl.io._
import org.http4s.multipart.{Multipart,Part}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.typelevel.ci.CIString
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
//
import java.io.ByteArrayInputStream
import java.util.UUID
//
import concurrent.duration._
import language.postfixOps

object UploadController {


  def controller(uphs:UploadHeaders)(authReq:AuthedRequest[IO,User])(implicit ctx:NodeContext) = for {
    arrivalTimeNanos    <- IO.monotonic.map(_.toNanos)
    currentState        <- ctx.state.get
    events              = Events.relativeInterpretEventsMonotonic(currentState.events)
    usedStorageCap      = EventXOps.calculateUsedStorageCapacity(events=events,nodeId = ctx.config.nodeId)
    availableStorageCap = ctx.config.totalStorageCapacity - usedStorageCap
    _                   <- ctx.logger.debug(s"USED_STORAGE_CAPACITY $usedStorageCap")
    _                   <- ctx.logger.debug(s"AVAILABLE_STORAGE_CAPACITY $availableStorageCap")
    responses           <- if(availableStorageCap >= uphs.objectSize) {
      for {
        _                 <- IO.unit
        maybeCompletedPut = EventXOps.completedPutByOperationId(events = events, operationId = uphs.operationId)
        responses         <- maybeCompletedPut match {
          case Some(value) =>
            Forbidden(s"${uphs.operationId} was completed", Headers(Header.Raw(CIString("Error-Msg"),s"${uphs.operationId} was completed")) ).map(_.asLeft[Response[IO]])
          case None => for {
            _           <- IO.unit
            req         = authReq.req
            user        = authReq.context
            multipart   <- req.as[Multipart[IO]]
            parts       = multipart.parts
            //    _______________________________________________
            responses   <- parts.traverse{ part =>
              for{
                _               <- IO.unit
                partHeaders     = part.headers
                contentType     = partHeaders.get(HEADERS.`Content-Type`.headerInstance.name).map(_.head.value).getOrElse("application/octet-stream")
                media           = MediaType.unsafeParse(contentType)
                objectExtension = media.fileExtensions.head
                body            = part.body

                metadata = Map(
                  "objectId" -> uphs.objectId,
                  "objectSize"-> uphs.objectSize.toString,
                  "extension" -> objectExtension,
                  "filePath" -> uphs.filePath,
                  "compressionAlgorithm" -> uphs.compressionAlgorithm,
                  "catalogId" -> uphs.catalogId,
                  "digest" -> uphs.digest,
                  "blockIndex" -> uphs.blockIndex.toString,
                  "blockId" -> uphs.blockId,
                  "contentType" -> contentType,
                  "blockTotal" -> uphs.blockTotal.toString
                )
                newObject <- if(!ctx.config.inMemory) {
                  for {
                    _    <- IO.unit
                    path = Paths.get(s"${ctx.config.storagePath}/${uphs.objectId}")
                    o    = ObjectD(guid=uphs.objectId,path =path,metadata=metadata).asInstanceOf[IObject]
                    _    <- body.through(Files[IO].writeAll(path)).compile.drain
                  } yield o
                }
                else {
                  for {
                    bytesBuffer  <- body.compile.to(Array)
                    o = ObjectS(
                      guid     = uphs.objectId,
                      bytes    = bytesBuffer,
                      metadata  = metadata
                    ).asInstanceOf[IObject]
                  } yield o
                }
                //        PUT TO CACHE
                evictedElement  = CacheX.put(events = events,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy)
                now             <- IO.realTime.map(_.toMillis)

                newHeaders      <- evictedElement match {
                  case Some(evictedObjectId) => for {
                    maybeEvictedObject <- currentState.cache.lookup(evictedObjectId)
                    newHeades             <- maybeEvictedObject match {
                      case Some(evictedObject) => for {
                        _                <- IO.unit
                        evictedObjectBytes <- evictedObject match {
                          case ObjectD(guid, path, metadata) => for {
                            bytes  <- Files[IO].readAll(path,chunkSize = 8192).compile.to(Array)
                          } yield bytes
                          case ObjectS(guid, bytes, metadata) => bytes.pure[IO]
                        }
                        evictedObjectSize = evictedObjectBytes.length
                        delete = evictedObject match {
                          case _:ObjectD => true
                          case _:ObjectS => false
                        }
                        _ <- Helpers.pushToNextLevel(
                          evictedObjectId = evictedObjectId,
                          bytes = evictedObjectBytes,
                          metadata = evictedObject.metadata,
                          currentEvents = events,
                          correlationId = uphs.operationId,
                          delete = delete
                        ).start
                        deleteStartAtNanos     <- IO.monotonic.map(_.toNanos)
                        _                      <- currentState.cache.delete(evictedObjectId)
                        deleteEndAt            <- IO.realTime.map(_.toMillis)
                        deleteEndAtNanos       <- IO.monotonic.map(_.toNanos)
                        deleteServiceTimeNanos = deleteEndAtNanos - deleteStartAtNanos
                        _                      <- currentState.cache.insert(uphs.objectId,newObject)

                        delEvent = Del(
                          serialNumber = 0 ,
                          nodeId = ctx.config.nodeId,
                          objectId = evictedObjectId,
                          objectSize = evictedObjectSize,
                          timestamp =deleteEndAt,
                          serviceTimeNanos = deleteServiceTimeNanos,
                          correlationId = uphs.operationId
                        )
                        _ <- Events.saveEvents(List(delEvent))
                        newHeaders = Headers(
                          Header.Raw(CIString("Evicted-Object-Id"),evictedObjectId),
                          Header.Raw(CIString("Evicted-Object-Size"),evictedObjectSize.toString),
                          Header.Raw(CIString("Node-Id"),ctx.config.nodeId ),
                          Header.Raw(CIString("Level"),"CLOUD"  ),
                          Header.Raw(CIString("Object-Size"),uphs.objectSize.toString)
                        )
                        //                      EVICTED
                        _ <- ctx.config.pool.sendEvicted(delEvent).start
                      } yield newHeaders
                      case None => for {
                        _ <- ctx.logger.error("WARNING: OBJECT WAS NOT PRESENT IN THE CACHE.")
                      } yield Headers.empty
                    }
                  } yield newHeades
                  //               NO EVICTION
                  case None => for {
                    //             PUT NEW OBJECT
                    _                   <- currentState.cache.insert(uphs.objectId,newObject)
                    newHeaders = Headers(
                      Header.Raw(CIString("Node-Id"),ctx.config.nodeId),
                      Header.Raw(CIString("Level"), "LOCAL"),
                      Header.Raw(CIString("Object-Size"),uphs.objectSize.toString)
                    )
                    //                    Headers.empty
                  } yield newHeaders
                }
                now                 <- IO.realTime.map(_.toMillis)
                nowNanos            <- IO.monotonic.map(_.toNanos)
                //              putServiceTime      = now - arrivalTime
                putServiceTimeNanos = nowNanos - arrivalTimeNanos
                responsePayload = PushResponse(
                  userId= user.id,
                  guid=uphs.objectId,
                  objectSize=  uphs.objectSize,
                  serviceTimeNanos = putServiceTimeNanos,
                  timestamp = now,
                  level  = ctx.config.level,
                  nodeId = ctx.config.nodeId
                ).asJson
                response <- Ok(responsePayload,newHeaders)
              } yield response
            }.map(_.head)
            //          .asRight[Response[IO]]
          } yield responses.asRight[Response[IO]]
        }
      } yield responses
    }
    else {
      for {
        _   <- ctx.logger.debug(s"NO_AVAILABLE_STORAGE_CAPACITY ${uphs.objectId}")
        res <- Accepted().map(_.asLeft[Response[IO]])
      } yield res
    }


    } yield responses

  def downloadIfPivotNode(uphs:UploadHeaders,value:ReplicationProcess)(implicit ctx:NodeContext) = {
    if(uphs.pivotReplicaNode == ctx.config.nodeId) value.what.traverse{ f =>
      val downloadReq = Request[IO](
        method = Method.GET,
        uri = Uri.unsafeFromString(f.url)
      )
      val path = Paths.get(s"${ctx.config.storagePath}/${f.id}")
      val response = if(path.toFile.exists()) Stream.empty else {
        ctx.client.stream(req = downloadReq).flatMap{ res =>
          res.body.through(Files[IO].writeAll(path = path))
        }
      }

      response.compile.drain
    }.void else IO.unit
  }
  def apply(s:Semaphore[IO])(implicit ctx:NodeContext): AuthedRoutes[User, IO] = {

    AuthedRoutes.of[User,IO]{
      case authReq@POST -> Root / "data" as user => for {
        _          <- ctx.logger.debug("DATA_WRITE")
        req        = authReq.req
        multipart  <- req.as[Multipart[IO]]
        objectIds  <- multipart.parts.traverse{ part =>
          val name    = part.name.getOrElse(s"${UUID.randomUUID().toString}")
          val body    = part.body
          val headers = part.headers
          val path    = Paths.get(s"${ctx.config.storagePath}/$name")
          body.through(Files[IO].writeAll(path = path)).compile.drain *> name.pure[IO]
        }
        _       <- ctx.config.pool.uploadCompletedv2(objectIds = objectIds.toList).start
        res     <- NoContent()
      } yield res
//      objectId -> snId
      case authReq@POST -> Root / "upload" as user =>
        val defaultConv = (x:FiniteDuration) => x.toNanos
        val program = for {
          serviceTimeStart     <- IO.monotonic.map(defaultConv).map(_ - ctx.initTime)
          serviceTimeStartReal <- IO.realTime.map(_.toNanos)
          //     ________________________________________________________________
          req                     = authReq.req
          headers                 = req.headers
          uphs                    <- UploadHeadersOps.fromHeaders(headers=headers)
          payload                 <- req.as[Map[String,ReplicationProcess]].onError(e=> ctx.logger.error(e.getMessage))
          maybeReplicationProcess = payload.get(ctx.config.nodeId)


          response                <- maybeReplicationProcess match {
            case Some(value) =>  for {
              _            <- IO.unit
              _            <- downloadIfPivotNode(uphs = uphs, value= value)
              //            __________________________________________________________________________________________________________
              replicaNodes = value.where
              _ <- ctx.logger.debug(s"REPLICA_NODES $replicaNodes")
              //            __________________________________________________________________________________________________________
              _  <- ctx.state.update{
                s=>
                  val newMetadata = value.what.map{w=>
                    val path = Paths.get(s"${ctx.config.storagePath}/${w.id}")
                    w.id ->  ObjectD(w.id,path =path,metadata = w.metadata )
                  }
                s.copy(metadata = s.metadata ++ newMetadata )
              }

              uploadRequests = replicaNodes.map { id =>
                val r0     = Request[IO](method = Method.POST, uri = Uri.unsafeFromString(s"http://$id:6666/api/v2/upload")).withEntity(payload)
//              ____________________________________________________________________________________
                val parts  = value.what.map{ w =>
                  val file = Paths.get(s"${ctx.config.storagePath}/${w.id}").toFile

                  Part.fileData[IO](
                    name    = w.id,
                    file    = file,
                    headers = Headers(Header.Raw(CIString("Object-Id"),w.id))
                  )

                }.toVector
//              ____________________________________________________________________________________
                val multipart = Multipart[IO](parts = parts)
                val r1 = Request[IO](
                  method  = Method.POST,
                  uri     = Uri.unsafeFromString(s"http://$id:6666/api/v2/data"),
                  headers = req.headers
                )
                  .withEntity(multipart)
                  .withHeaders(multipart.headers)
                (r0,r1)
              }
              //             _________________________________________________________________________________________________________
              pushReplicasAndData = for {
                  _ <- IO.unit
                  x <- uploadRequests.traverse{
                    case (upReq,dataReq)  =>
                      ctx.client.status(dataReq).flatMap(s=> ctx.logger.debug(s"DATA_STATUS $s")) *> ctx.client.status(upReq).flatMap(s=> ctx.logger.debug(s"UPLOAD_STATUS $s"))
                  }
                  res <- Ok()
                } yield res

              res <- value.how.technique match {
                case "ACTIVE" =>
                  value.how.transferType match {
                    case "PUSH" => pushReplicasAndData
                    case "PULL" => Forbidden("NO_IMPLEMENTATION_YET")
                  }
                case "PASSIVE" =>
                  value.how.transferType match {
                    case "PUSH" => pushReplicasAndData
                    case "PULL" => Forbidden("NO_IMPLEMENTATION_YET")
                  }
              }
            } yield res
            case None => NoContent()
          }
          _ <- ctx.logger.debug(s"RESPONSE $response")
      } yield response

        program.onError{ e=>
          ctx.logger.error(e.getMessage) *> ctx.logger.debug("____________________________________________________")
        }
    }
  }

}
