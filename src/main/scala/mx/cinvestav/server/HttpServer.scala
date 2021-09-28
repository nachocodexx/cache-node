package mx.cinvestav.server

import fs2.io.file.Files
import mx.cinvestav.commons.types.ObjectLocation
//
import cats.implicits._
import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import cats.nio.file.{Files => NIOFiles}
//
import java.nio.file.Paths
//
import mx.cinvestav.Declarations.NodeContextV5
import mx.cinvestav.cache.cache.CachePolicy
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.types.ObjectMetadata
import mx.cinvestav.server.Routes.UploadResponse
import mx.cinvestav.Declarations.RequestX
import mx.cinvestav.Helpers
import mx.cinvestav.utils.v2.encoders._
import mx.cinvestav.commons.{status=>StatuX}
import mx.cinvestav.commons.stopwatch.StopWatch._
//
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.{AuthMiddleware, Router}
import org.http4s.{AuthedRoutes, HttpRoutes, Request, Response}
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.multipart.Multipart
import org.typelevel.ci._
import org.http4s.blaze.client.BlazeClientBuilder
//
import io.circe.generic.auto._
import io.circe.syntax._
//
//
import java.util.UUID
import scala.concurrent.ExecutionContext.global
import concurrent.duration._
import language.postfixOps
import org.http4s._

object HttpServer {

  case class User(id:UUID,bucketName:String)


//  def UploadOnUp(operationId:Int,req:Request[IO],user: User)(implicit ctx:NodeContextV5) = for {
//    currentState         <- ctx.state.get
//    timestamp            <- IO.realTime.map(_.toMillis)
//    nodeId               = ctx.config.nodeId
//    poolId               = ctx.config.poolId
//    port                 = ctx.config.port
//    storagePath          =  ctx.config.storagePath
//    ip                   = currentState.ip
//    baseUrl              = s"http://${ip}:${port}"
//    cacheNodes           = currentState.cacheNodePubs
////    REQUEST
//    payload              <- req.as[Multipart[IO]]
////   CACHE
//    cacheX               = CachePolicy(ctx.config.cachePolicy)
////
//    parts                = payload.parts
//    headers              = req.headers
//    compressionAlgorithm = headers.get(CIString("Compression-Algorithm")).map(_.head).map(_.value).map(_.toUpperCase()).getOrElse("")
//    //
//    currentOperationId   = currentState.currentOperationId
//    _                    <- ctx.state.update(_.copy(currentOperationId = operationId.some ))
//    //
//    ca                   = compression.fromString(compressionAlgorithm)
//    userId               =  user.id
//    bucketName           =  user.bucketName
//    //
//    baseStr         =  s"$storagePath/$nodeId/$userId/$bucketName"
//    basePath        =  Paths.get(baseStr)
//    _               <- NIOFIles[IO].createDirectories(basePath)
//    //      _                    <- headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
//    _                    <- parts.traverse{ part=>
//      for {
//        _                <- IO.unit
//        cache            = currentState.cache
//        _                <- part.headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
//        partHeaders      = part.headers
//        //         Metadata
//        guid             = partHeaders.get(key =ci"guid").map(_.head).map(_.value).map(UUID.fromString).getOrElse(UUID.randomUUID())
//        contentType      = partHeaders.get(ci"Content-Type").map(_.head.value).getOrElse( "application/octet-stream")
//        originalFilename = part.filename.getOrElse("default")
//        originalName     = part.name.getOrElse("default")
//        checksum         = ""
//        metadata         = ObjectMetadata(
//          guid =  guid,
//          size  = 0L,
//          compression = ca.token,
//          bucketName = bucketName,
//          filename = originalFilename,
//          name = originalName,
//          locations = Nil,
//          policies = Nil,
//          checksum = checksum,
//          userId = userId.toString,
//          //              .getOrElse(UUID.randomUUID().toString),
//          version = 1,
//          timestamp = timestamp,
//          contentType = contentType,
//          extension = ""
//        )
//        //
//        sinkPath        =  Paths.get(baseStr+s"/$guid")
//        stream           = part.body
//        writeFile = stream.through(Files[IO].writeAll(path=sinkPath)).compile.drain
//        //
//        _                <- ctx.logger.debug("GUID "+guid.toString)
//        putRes           <- cacheX.putv2(cache,guid.toString)
////      _____________________________________________________________________
//        _                <- putRes.evicted match {
//          case Some(element) =>  for {
//            _             <- IO.unit
//            _cacheNodes   = cacheNodes.filter(_._1!=nodeId)
//            cachePubs     = _cacheNodes.values.toList
//            _cacheNodeIds = _cacheNodes.keys.toList
//            _             <- ctx.logger.debug("CACHE_NODES "+_cacheNodeIds.mkString("//"))
//            //            Transaction
//            transactionId = guid.toString
//            //                UUID.randomUUID().toString
//            transaction   = CacheTransaction(
//              id = transactionId,
//              cacheNodes =_cacheNodeIds,
//              timestamp = timestamp,
//              proposedElement = ProposedElement(element.key,element.value),
//              nodeId = nodeId,
//              data = stream,
//              userId = userId,
//              bucketName = bucketName,
//              filename = FileMetadata.fromPath(Paths.get ( originalFilename) ).filename.value,
//              compressionAlgorithm=ca
//            )
//            //            ADD_TRANSACTION_LOCAL
//            _             <- ctx.state.update(s=>s.copy(transactions = s.transactions+ (transactionId->transaction)))
//            //            SEND PREPARE TO CACHE_NODES
//            payload       = Payloads.Propose(
//              guid = transactionId,
//              url="URL",
//              timestamp=timestamp,
//              proposedElement = ProposedElement(element.key,element.value)
//            ).asJson.noSpaces
////
//            properties = AmqpProperties(
//              headers = Map("commandId" -> StringVal(CommandIds.PROPOSE)  ),
//              replyTo = nodeId.some
//            )
////
//            msg        = AmqpMessage[String](payload=payload,properties=properties)
////
//            implicit0(rabbitMQContext:RabbitMQContext) <- IO.pure(ctx.rabbitMQContext)
//            connection                      = rabbitMQContext.connection
//            client                          = rabbitMQContext.client
//            (channel,finalizer)             <- client.createChannel(connection).allocated
//            implicit0(_channel:AMQPChannel) <- IO.pure(channel)
//            _                               <- cachePubs.traverse(_.publishWithChannel(msg))
//            _                               <- finalizer
//            _          <- ctx.logger.debug("INIT PAXOS")
//          } yield ()
//          case None => for {
//            _        <- ctx.logger.debug(s"NO EVICTION $ca")
//            _        <- writeFile
//            _        <- ctx.logger.debug(s"AFTER_WRITE")
//            _        <- compression.compress(ca=ca,source = sinkPath.toString,baseStr)
//              .value.flatMap {
//              case Left(e) => ctx.logger.error(e.getMessage)
//              case Right(value) => for {
//                _        <- ctx.logger.debug(value.toString)
//                _        <- NIOFIles[IO].delete(sinkPath)
//                endTime  <- IO.realTime.map(_.toMillis)
//                duration = endTime-timestamp
//                _        <- ctx.logger.info(s"UPLOAD $guid $duration")
//              } yield ()
//            }.onError{ t=> ctx.logger.error(t.getMessage)}
//          } yield ()
//        }
//        //          UPDATE CACHE
//        _                <- ctx.state.update(_.copy(cache=putRes.newCache))
//        _ <- ctx.logger.debug("______________________________________________________________________")
//      } yield ()
//    }
//    //     WRITE TO DISK
//    //
//    endedAt   <- IO.realTime.map(_.toMillis)
//    payload   = UploadResponse(operationId = UUID.randomUUID().toString ,uploadSize = 0L,duration = endedAt-timestamp )
//    response  <- Ok("RESPONSE")
//
//  } yield ()


  def authUser()(implicit ctx:NodeContextV5):Kleisli[OptionT[IO,*],Request[IO],User] =
    Kleisli{ req=> for {
      _          <- OptionT.liftF(ctx.logger.debug("AUTH MIDDLEWARE"))
      headers    = req.headers
      _          <- OptionT.liftF(ctx.logger.debug(headers.headers.mkString(" // ")))
      maybeUserId     = headers.get(ci"User-Id").map(_.head).map(_.value)
      maybeBucketName = headers.get(ci"Bucket-Id").map(_.head).map(_.value)
      _          <- OptionT.liftF(ctx.logger.debug(maybeUserId.toString+"//"+maybeBucketName.toString))
      ress            <- (maybeUserId,maybeBucketName) match {
        case (Some(userId),Some(bucketName)) =>   for {
          x  <- OptionT.liftF(User(id = UUID.fromString(userId),bucketName=bucketName  ).pure[IO])
//          _  <- OptionT.liftF(ctx.logger.debug("AUTHORIZED"))
        } yield x
        case (Some(_),None) => OptionT.none[IO,User]
        case (None,Some(_)) => OptionT.none[IO,User]
        case (None,None )   => OptionT.none[IO,User]
      }

      } yield ress
    }
  def authMiddleware(implicit ctx:NodeContextV5):AuthMiddleware[IO,User] =
    AuthMiddleware(authUser=authUser)

  def authRoutes()(implicit ctx:NodeContextV5):AuthedRoutes[User,IO]  = AuthedRoutes.of{
    case req@GET -> Root as user => for {
      _   <- ctx.logger.debug(user.toString)
      res <- Ok("TOP SECRET")
    } yield res
    case authReq@GET -> Root /"download"/ UUIDVar(guid)  as user =>for {

      timestamp            <- IO.realTime.map(_.toMillis)
      currentState         <- ctx.state.get
      cachePolicy          = ctx.config.cachePolicy
      nodeId               = ctx.config.nodeId
      poolId               = ctx.config.poolId
      port                 = ctx.config.port
      storagePath          =  ctx.config.storagePath
      _                    <- IO.unit
      cache                = currentState.cache
      cacheX               = CachePolicy(cachePolicy)
      req                  = authReq.req
      key                  = guid.toString
      headers              = req.headers
      compressionAlgorithm = headers.get(CIString("Compression-Algorithm")).map(_.head).map(_.value).map(_.toUpperCase()).getOrElse("")
      extension            = headers.get(CIString("File-Extension")).map(_.head).map(_.value).getOrElse("")
      fileDecompress       = headers.get(CIString("File-Decompress")).map(_.head).exists(_.value == "true")
      operationId          = headers.get(CIString("Operation-Id")).flatMap(_.head.value.toIntOption).getOrElse(0)
      ca                   = compression.fromString(compressionAlgorithm)
      userId               =  user.id
      bucketName           =  user.bucketName
      baseStr              =  s"$storagePath/$nodeId/$userId/$bucketName"
      basePath             =  Paths.get(baseStr)
      tempGuid             = guid
      tempFolderPath       = basePath.resolve(s"temp-$tempGuid")
      tempFileStr          = basePath+s"/$tempGuid.$extension"
      tempFilePath         = Paths.get(tempFileStr)
      sourcePath           = basePath.resolve(key+"."+ca.extension)
      sourceExits          <- NIOFiles[IO].exists(sourcePath)
      tempExists           <- NIOFiles[IO].exists(tempFilePath)
      cacheAction          = for {
        readRes <- cacheX.read(cache,key)
        _       <- ctx.state.update(_.copy(cache = readRes.newCache))
      } yield readRes
      _ <- ctx.logger.debug(s"SOURCE_EXISTS ${sourceExits} - FILE_DECOMPRESS ${fileDecompress}")
//
      cacheResult <-cacheAction

      response2 <-  if(sourceExits && !fileDecompress ) {
        for {
          _<- ctx.logger.debug("IF -> EXISTS A THE COMPRESSED OBJECT and File-Decompression = false")
          _ <- ctx.logger.info(s"HIT $guid ${cacheResult.value}")
          stream_   = Files[IO].readAll(sourcePath,8192)
          response <- Ok(stream_)
        } yield response
      }
//
      else if(sourceExits && fileDecompress){
        for {
          _ <- ctx.logger.debug("IF -> EXISTS A THE COMPRESSED OBJECT and File-Decompression = true")
          _ <- ctx.logger.info(s"HIT $guid ${cacheResult.value}")
          res <- if(!tempExists)  for {
            _   <- NIOFiles[IO].createDirectories(tempFolderPath)
            res <- compression.decompress(ca,sourcePath.toString,tempFolderPath.toString ).value.flatMap {
              case Left(e) => ctx.logger.error(e.getMessage) *> InternalServerError()
              case Right(value) => for {
                _    <- ctx.logger.debug(value.toString)
                //            source = Paths.get(basePath.toString+s"/temp/$guid")
                source  = tempFolderPath.resolve(guid.toString)
                target  = Paths.get(tempFileStr)
                //                 _ <- ctx.logger.debug(s"MOVE_SOURCE $source")
                //                 _ <- ctx.logger.debug(s"MOVE_TARGET $target")
                newPath <- NIOFiles[IO].move(source, target)
                //                 _ <- ctx.logger.debug(s"MOVE_NEW_PATH $newPath")
                stream = Files[IO].readAll(newPath,8192)
                clean = for {
                  _ <- ctx.logger.debug("CLEAN")
                  _ <- NIOFiles[IO].delete(tempFolderPath)
                  _ <- NIOFiles[IO].delete(tempFilePath)
                  //                   _ <- NIOFIles[IO].delete(target)
                  _ <- ctx.logger.debug("CLEAN_DONE")
                } yield ()
                _ <- (IO.sleep(30 seconds) *> clean).start
                res    <- Ok(stream)
              } yield res
            }
          } yield res
          //      EXISTS A DECOMPRESS VERSION OF THE FILE
          else for {
            _        <- ctx.logger.debug("EXISTS A UNCOMPRESSED FILE")
            stream_   = Files[IO].readAll(tempFilePath,8192)
            response <- Ok(stream_)
          } yield response

        } yield res
      }
//    OBJECT NOT FOUND
      else{
        for {
          _                  <- ctx.logger.info(s"MISS $guid")
//          _ <- IO.unit
          chordNode          = ctx.config.keyStore.nodes.head
          chordGetRes        <- chordNode.get(guid.toString).stopwatch
          chordGetResTime    = chordGetRes.duration.toMillis
          _                  <- ctx.logger.info(s"GET_METDATA $guid $chordGetResTime")
          metadataResult     = chordGetRes.result
          locations          = metadataResult.value.locations
          (client,finalizer2) <- BlazeClientBuilder[IO](global).resource.allocated
          req = Request[IO](
            method  = Method.GET,
            uri     = Uri.unsafeFromString(locations.head.url),
            headers = Headers(
              Header.Raw(CIString("User-Id"),userId.toString),
              Header.Raw(CIString("Bucket-Id"),bucketName),
              Header.Raw(CIString("Compression-Algorithm"),ca.token),
              Header.Raw(CIString("File-Extension"),extension),
              Header.Raw(CIString("Operation-Id"),operationId.toString),
              Header.Raw(CIString("File-Decompress"),fileDecompress.toString),
            )
          )
          _              <- ctx.logger.debug(req.headers.headers.mkString("\n"))
          metadataResult <- client.toHttpApp.run(req).stopwatch
          responseS      = metadataResult.result
          status         = responseS.status
          _              <- ctx.logger.debug(s"GET_METADATA $guid ${metadataResult.duration.toMillis}")
          _              <- ctx.logger.debug(s"STATUS $status")
          response       = responseS
          //          Ok(responseS)
          _ <- finalizer2
        } yield response
      }
//
//      cache   = currentState.cache
//      cacheX  = CachePolicy(cachePolicy)
//      readRes <- cacheX.read(cache,key)
      endAt <- IO.realTime.map(_.toMillis)
      duration = endAt - timestamp
      _ <- ctx.logger.debug(s"DOWNLOAD $guid $duration")
      _ <- ctx.logger.debug("______________________________________________________________________")
    } yield response2

    case authReq@POST -> Root / "upload" as user => for {
      timestamp     <- IO.realTime.map(_.toMillis)
      currentState  <- ctx.state.get
      queue         = currentState.queue
      status        = currentState.status
      req           = authReq.req
      headers       = req.headers
      operationId   = headers.get(CIString("Operation-Id")).map(_.head).map(_.value).flatMap(_.toIntOption).getOrElse(0)
        _           <- Helpers.onUp(operationId,authReq)
      response      <- Ok("HERE!")
    } yield response


    case authReq@POST -> Root / "uploadv2" as user => for {
      _ <- ctx.logger.debug("INIT_UPLOAD_v2")
      timestamp            <- IO.realTime.map(_.toMillis)
      currentState         <- ctx.state.get
      req                  = authReq.req
      nodeId               = ctx.config.nodeId
      poolId               = ctx.config.poolId
      port                 = ctx.config.port
      storagePath          =  ctx.config.storagePath
      ip                   = currentState.ip
      baseUrl              = s"http://${ip}:${port}"
      cacheNodes           = currentState.cacheNodePubs
      payload              <- req.as[Multipart[IO]]
      cacheX               = CachePolicy(ctx.config.cachePolicy)
      parts                = payload.parts
      headers              = req.headers
      //      compressionAlgorithm = headers.headers.find(_.name===`Accept-Encoding`.headerInstance.name).map(_.value.toUpperCase).getOrElse(compression.LZ4)
      compressionAlgorithm = headers.get(CIString("Compression-Algorithm")).map(_.head).map(_.value).map(_.toUpperCase()).getOrElse("")
      userId               =  user.id
      bucketName           =  user.bucketName
      //
      baseStr         =  s"$storagePath/$nodeId/$userId/$bucketName"
      basePath        =  Paths.get(baseStr)
      _               <- NIOFiles[IO].createDirectories(basePath)
      //      _                    <- headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
      _                    <- parts.traverse{ part=>
        for {
          _                <- IO.unit
          cache            = currentState.cache
          _                <- part.headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
          partHeaders      = part.headers
          //         Metadata
          guid             = partHeaders.get(key =ci"guid").map(_.head).map(_.value).map(UUID.fromString).getOrElse(UUID.randomUUID())
//          contentType      = partHeaders.get(ci"Content-Type").map(_.head.value).getOrElse( "application/octet-stream")
//          originalFilename = part.filename.getOrElse("default")
//          originalName     = part.name.getOrElse("default")
//          checksum         = ""
          ca               = compression.fromString(compressionAlgorithm)
          _ <- ctx.logger.debug("AFTER_GET_METADATA")
//
          chordNode = ctx.config.keyStore.nodes.head
          metadataGetResponse  <- chordNode.get(guid.toString)
          _                    <- ctx.logger.debug(metadataGetResponse.toString)
          metadata             = metadataGetResponse.value
          locations = metadata.locations
          newLocations = locations :+ ObjectLocation(nodeId=nodeId,poolId=poolId,url=baseUrl+s"/download/$guid")
          newMetdata = metadata.copy(
            locations =  newLocations
          )
          putStatus<-  chordNode.put(guid.toString,newMetdata.asJson.noSpaces)
          _ <- ctx.logger.debug(s"PUT_STATUS $putStatus")
          //
          sinkPath         =  Paths.get(baseStr+s"/$guid.${ca.extension}")
          stream           = part.body
          writeFile        = stream.through(Files[IO].writeAll(path=sinkPath)).compile.drain
          // CACHE
          _                <- ctx.logger.debug("GUID "+guid.toString)
          putRes           <- cacheX.put(cache,guid.toString)
          maybeEvictedItem          = putRes.evicted
          _ <- maybeEvictedItem match {
            case Some(value) => for {
              //              _                <- NIOFIles[IO].delete(evictedPath)
              _            <- IO.unit
              evictedPath  = Paths.get(baseStr+s"/${value.key}.${ca.extension}")
              evictedExits <- NIOFiles[IO].exists(evictedPath)
              syncNode     = currentState.syncLB.balance(rounds = 1).head
              syncNodePub  = currentState.syncNodePubs(syncNode)
              _<- ctx.logger.debug(s"EVICTED_PATH $evictedPath")
              _<- ctx.logger.debug(s"EVICTED_EXISTS $evictedExits")
              _<- ctx.logger.debug(s"SYNC_NODE $syncNode")
//              _<- ctx.logger.debug(s"SYNC_NODE_PUB $syncNodePub")
              _ <- Helpers.sendPull(userId.toString,bucketName,value.key,syncNodePub,evictedPath)
              _   <- writeFile
              _   <- ctx.state.update(_.copy(
                status=StatuX.Up,
                currentOperationId = None,
                cache = putRes.newCache
              ))
            } yield ()
            case None =>  for {
              _        <- ctx.logger.debug("NO EVICTION")
              _        <- writeFile
              _        <- ctx.state.update(_.copy(cache=putRes.newCache))
            } yield ()
          }
          _                <- ctx.logger.debug("______________________________________________________________________")

        } yield ()
      }
      //     WRITE TO DISK
      //
      endedAt   <- IO.realTime.map(_.toMillis)
      payload   = UploadResponse(operationId = UUID.randomUUID().toString ,uploadSize = 0L,duration = endedAt-timestamp )
      response  <- Ok("RESPONSE")
    } yield response


  }

  private def httpApp()(implicit ctx:NodeContextV5): Kleisli[IO, Request[IO],
    Response[IO]] =
    Router[IO](
//      "/" -> Routes(),
      "/" ->  authMiddleware(ctx=ctx)(authRoutes()),
      "/stats" -> HttpRoutes.of[IO]{
        case req@GET -> Root / "v2" => for {
          _        <- ctx.logger.debug("HEREEE!")
          background = for {
            _ <-  ctx.logger.debug("AQUI!")
            _ <- Helpers.sendRequest(req)
          } yield ()
          _ <- (IO.sleep(2 seconds) *> background).start
          response <- Ok("RESPONSE")
        } yield response
        case req@GET -> Root => for {
          currentState <- ctx.state.get
          timestamp    <- IO.realTime.map(_.toMillis)
          nodeId       = ctx.config.nodeId
          poolId       = ctx.config.poolId
          ip           = currentState.ip
          port         = ctx.config.port
          keys         <- currentState.currentEntries.get
          values       <- keys.traverse(key=>
            currentState.cache.lookup(key).map(v=>(key,v.getOrElse(0)))
          ).map(_.toMap)
          data         = Map(
            "NODE_ID"->nodeId.asJson,
            "POOL_ID"->poolId.asJson,
            "IP_ADDRESS"->ip.asJson,
            "NODE_PORT" -> port.asJson,
            "DATA" -> values.asJson,
            "TIMESTAMP" -> timestamp.asJson
          )
          response     <- Ok(data.asJson)
        } yield (response)
      }
    ).orNotFound

  def run()(implicit ctx:NodeContextV5): IO[Unit] = for {
    _ <- ctx.logger.debug(s"HTTP SERVER AT ${ctx.config.host}:${ctx.config.port}")
    _ <- BlazeServerBuilder[IO](executionContext = global)
    .bindHttp(ctx.config.port,ctx.config.host)
    .withHttpApp(httpApp = httpApp())
    .serve
    .compile
    .drain
  } yield ()

}
