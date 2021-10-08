package mx.cinvestav.server
// Cats
import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.objectSEncoder
import org.http4s.{MediaType, Method}
import org.http4s.blaze.client.BlazeClientBuilder

import scala.concurrent.ExecutionContext.global
// Cats NIO
import cats.nio.file.{Files=>NIOFiles}
// Fs2
import fs2.io.file.Files
// Local
import mx.cinvestav.Declarations.{NodeContextV5, ObjectS, User}
import mx.cinvestav.Helpers
// Http4s
import org.http4s.{Header, Headers, Request, Uri}
import org.http4s.{headers=>HEADERS}
import org.http4s.multipart.{Multipart, Part}
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.AuthedRoutes
import org.typelevel.ci.CIString
import org.http4s.HttpRoutes
import org.http4s.implicits._
import org.http4s.dsl.io._
// Circe
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
// Native
import java.nio.file.Paths
import java.util.UUID

object RouteV6 {

  case class PushResponse(
                           nodeId:String,
                           userId:String,
                           guid:String,
                           objectSize:Long,
                           milliSeconds:Long,
                           timestamp:Long,
                           level:Int
//                           evictedItem
                         )

  def apply()(implicit ctx:NodeContextV5): AuthedRoutes[User, IO] =  AuthedRoutes.of[User,IO]{
    case authReq@POST -> Root / "upload" as user => for {
      arrivalTime  <- IO.realTime.map(_.toMillis)
      currentState <- ctx.state.get
      currentLevel = ctx.config.level
      cache        = currentState.cacheX
      req          = authReq.req
      multipart    <- req.as[Multipart[IO]]
      parts        = multipart.parts
      responses    <- parts.traverse{ part =>
        for{
          _           <- ctx.logger.debug(part.headers.toString)
          partHeaders = part.headers
          guid        = partHeaders.get(CIString("guid")).map(_.head.value).getOrElse(UUID.randomUUID().toString)
          body        = part.body
          bytesBuffer <- body.compile.to(Array)
//            .through(Helpers.streamBytesToBuffer)
          rawBytesLen  = bytesBuffer.length
          _ <- ctx.logger.debug(s"RAW_BYTES $rawBytesLen")
          objectSize  = partHeaders
            .get(org.http4s.headers.`Content-Length`.name)
            .map(_.head.value)
            .getOrElse("0")
          _ <- ctx.state.update(
            s=>{
              val newUsedStorageSpace = s.usedStorageSpace+objectSize.toLong
              s.copy(
                usedStorageSpace = newUsedStorageSpace,
                availableStorageSpace = s.totalStorageSpace - newUsedStorageSpace
              )
            }
          )

          value       = ObjectS(
            guid=guid,
            bytes= bytesBuffer,
            metadata=Map("objectSize"->objectSize)
          )
          //        PUT TO CACHE
          putResponse <- cache.put(key=guid,value=value)
          //        CHECK IF EVICTION OCCUR
          response    <- putResponse.evictedItem match {
            case Some(evictedElem) =>  for {
              endAt0     <- IO.realTime.map(_.toMillis)
              //            get service time
              time0      = endAt0 - arrivalTime
              _          <- ctx.logger.info(s"PUSH_LOCAL $guid $time0")
//              Update storage of the node
              _          <- ctx.state.update( s=>{
                  val evictedObjectSize = evictedElem.value.metadata
                    .get("objectSize")
                    .flatMap(_.toLongOption)
                    .getOrElse(0L)
                  s.copy(
                    usedStorageSpace =  s.usedStorageSpace - evictedObjectSize,
                    availableStorageSpace =  s.availableStorageSpace + evictedObjectSize
                  )
                }

              )
//
              oneURL    = if(currentLevel ==0 ) ctx.config.loadBalancer.one.httpURL else ctx.config.loadBalancer.cloud.httpURL
              levelOneResponse        <- Helpers.evictedItemRedirectTo(req=req,oneURL=oneURL,evictedElem=evictedElem,user=user)
              _ <- ctx.logger.debug(s"LEVEL_ONE_RESPONSE $levelOneResponse")
              //            get timestamp
              endAt     <- IO.realTime.map(_.toMillis)
              //            get service time
              time      = endAt - arrivalTime
              responsePayload         = PushResponse(
                userId=user.id.toString,
                guid=guid,
                objectSize=  objectSize.toLong,
                milliSeconds = time,
                timestamp = endAt,
                level  = ctx.config.level,
                nodeId = ctx.config.nodeId
              )
//
              response <- Ok(responsePayload.asJson,
                  Headers(
                    Header.Raw(CIString("Evicted-Item"),evictedElem.key),
                  )
              )
//
            } yield response
            case None =>  for{
              endAt    <- IO.realTime.map(_.toMillis)
              time     = endAt - arrivalTime
              responsePayload = PushResponse(
                userId=user.id.toString,
                guid=guid,
                objectSize=  objectSize.toLong,
                milliSeconds = time,
                timestamp = endAt,
                level  = ctx.config.level,
                nodeId = ctx.config.nodeId
              )
              response <- Ok(responsePayload.asJson)
              _ <- ctx.logger.info(s"PUSH_${currentState.levelId} $guid $time")
//              else ctx.logger.info("")
            } yield response
          }
        } yield response
      }.map(_.head)
      _ <- ctx.logger.debug(responses.toString)
      _ <- ctx.logger.debug("____________________________________________________")
    } yield responses

    case authReq@POST -> Root / "replicate" / UUIDVar(guid) as user => for {
      currentState <- ctx.state.get
      cacheX               = currentState.cacheX
      req                  = authReq.req
      headers              = req.headers
//
      replicationStrategy  = headers.get(CIString("Replication-Strategy")).map(_.head.value).getOrElse("ACTIVE")
      replicationFactor    = headers.get(CIString("Replication-Factor")).map(_.head.value).flatMap(_.toIntOption).getOrElse(0)
      replicaNodes         = headers.get(CIString("Replica-Node")).map(_.map(_.value))
//
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
            requests      = replicaNodes.map{ urls =>
              urls.map( url =>
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
              )
            }
            (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
            responses          <- requests.traverse(_.traverse(req=>client.toHttpApp.run(req)))
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

    case authReq@GET -> Root / "download" / UUIDVar(guid) as user => for {
      arrivalTime   <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      currentLevel  = ctx.config.level
      currentNodeId = ctx.config.nodeId
      cache         = currentState.cacheX
      req           = authReq.req
      oneLevelURL   = ctx.config.loadBalancer.one.httpURL
      zeroLevelURL  = ctx.config.loadBalancer.zero.httpURL
      getResponse   <- cache.get(guid.toString)
      response      <- getResponse.item match {
        case Some(element) => for {
          _     <- ctx.logger.info(s"HIT $guid ${element.counter}")
          streamBuffer = element.value.bytes
          streamBytes  = fs2.Stream.emits(streamBuffer).covary[IO]
          bytesLen     =  streamBuffer.length
          _            <- ctx.logger.debug(s"RAW_BYTES $bytesLen")
          response <- Ok(
            streamBytes,
            Headers(
              Header.Raw(CIString("guid"), element.value.guid),
              Header.Raw(CIString("Object-Size"),element.value.metadata.getOrElse("objectSize","0")),
              Header.Raw(CIString("Level"),currentLevel.toString),
              Header.Raw(CIString("Node-Id"),ctx.config.nodeId)
            ),
          )

        } yield response
        case None => for {
          _                      <- ctx.logger.info(s"MISS $guid")
          response               <- if(currentLevel == 0 ) {
            for {
              _                      <- IO.unit
              //            GET ELEMENT FROM SYNC
              redirectRes            <- Helpers.redirectTo(oneLevelURL,req)
              _                      <-ctx.logger.debug(s"PULL_SYNC_RESPONSE $redirectRes")
              level1Bytes            = redirectRes.body.covary[IO]
              rawBytesBuffer         <- level1Bytes.compile.to(Array)
              bytesLen               = rawBytesBuffer.length
              _                      <- ctx.logger.debug(s"RAW_BYTES $bytesLen")
              serviceTime0           <- IO.realTime.map(_.toMillis).map(_ - arrivalTime)
              _                      <- ctx.logger.info(s"PULL_SYNC $guid $serviceTime0")
              //              HEADERS
              level1ObjectGuidH       = redirectRes.headers.get(CIString("guid")).map(_.head).get
              level1ObjectObjectSizeH = redirectRes.headers.get(CIString("Object-Size")).map(_.head).get
              level1ObjectLevelH      = redirectRes.headers.get(CIString("Level")).map(_.head).get
              level1ObjectNodeIdH     = redirectRes.headers.get(CIString("Node-Id")).map(_.head).get
              newHeaders              = Headers(level1ObjectGuidH,level1ObjectObjectSizeH,level1ObjectLevelH,level1ObjectNodeIdH)
              //            VALUES
              level1ObjectGuid        = level1ObjectGuidH.value
              level1ObjectSize        = level1ObjectObjectSizeH.value.toLong
              _<- ctx.logger.debug("BEFORE PUT!")
              value                  = ObjectS(
                guid=level1ObjectGuid,
//                bytes= level1Bytes,
                  bytes = rawBytesBuffer,
//                    fs2.Stream.emits(rawBytes).covary[IO].through(Helpers.streamBytesToBuffer),
                metadata=Map("objectSize"->level1ObjectSize.toString)
              )
              //            PUSH NEW ELEMENT FROM SYNC
              putResponse            <- cache.put(key=level1ObjectGuid,value=value)
              asyncEviction          = for {
                _ <- putResponse.evictedItem match {
                  case Some(evictedItem) => for {
                    _         <- ctx.logger.debug(s"EVICTION $evictedItem")
                    _         <- Helpers.updateStorage(evictedItem,level1ObjectSize)
                    uploadReq = Request[IO](
                      method = Method.POST,
                      uri = Uri.unsafeFromString(s"$oneLevelURL/api/v6/upload"),
                    )
                    uploadResponse          <- Helpers.evictedItemRedirectTo(req=uploadReq,oneURL =oneLevelURL,evictedElem= evictedItem , user=user)
                    _                       <- ctx.logger.debug(s"UPLOAD_RESPONSE $uploadResponse")
                    updateSchemaReq = Request[IO](
                      method = Method.POST,
                      uri = Uri.unsafeFromString(s"$zeroLevelURL/api/v6/update-schema"),
                      headers = Headers(
                        Header.Raw(CIString("Evicted-Item-Id"),evictedItem.key),
                        Header.Raw(CIString("New-Item-Id"),level1ObjectGuid),
                        Header.Raw(CIString("Node-Id"),currentNodeId),
                        Header.Raw(CIString("User-Id"),user.id.toString ),
                        Header.Raw(CIString("Bucket-Id"),user.bucketName),

                      )
                    )
                    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
                    updateSchemaRes    <- client.status(updateSchemaReq)
                    _                  <- ctx.logger.debug(s"UPDATE_SCHEMA_STATUS $updateSchemaRes")
                    _                  <- finalizer

                  } yield ()
                  case None => for {
                    _ <- ctx.logger.debug("NO-EVICTION")
                    updateSchemaReq = Request[IO](
                      method = Method.POST,
                      uri = Uri.unsafeFromString(s"$zeroLevelURL/api/v6/update-schema"),
                      headers = Headers(
                        Header.Raw(CIString("New-Item-Id"),level1ObjectGuid),
                        Header.Raw(CIString("Node-Id"),currentNodeId),
                        Header.Raw(CIString("User-Id"),user.id.toString ),
                        Header.Raw(CIString("Bucket-Id"),user.bucketName),

                      )
                    )
                    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
                    updateSchemaRes    <- client.status(updateSchemaReq)
                    _                  <- ctx.logger.debug(s"UPDATE_SCHEMA_STATUS $updateSchemaRes")
                    _                  <- finalizer
                  } yield ()
                }
              } yield ()
              _ <- asyncEviction.start
              bytesStream = fs2.Stream.emits(rawBytesBuffer).covary[IO]
              newResponse <-  Ok(bytesStream,newHeaders)
          } yield newResponse
        }
//        CLOUD AND TRIGGER SYSTEM REP.
          else {
            for {
              _        <- ctx.logger.debug("GO TO CLOUD - TRIGGER SYSTEM REPLICATION")
              response <- NotFound()
            } yield response
          }
        } yield response
      }

      _ <- ctx.logger.debug("RESPONSE -> "+response.toString)
      _ <- ctx.logger.debug("____________________________________________________")
    } yield response

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
