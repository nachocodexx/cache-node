package mx.cinvestav.server

import fs2.io.file.Files
import fs2.compression.Compression
import mx.cinvestav.Declarations.RequestX
import mx.cinvestav.Helpers
import mx.cinvestav.commons.fileX.{FileMetadata, Filename}
//
import cats.implicits._
import cats.data.{Kleisli, OptionT}
import cats.effect.IO
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AMQPChannel, AmqpMessage, AmqpProperties}
import mx.cinvestav.Declarations.ProposedElement
import mx.cinvestav.utils.v2.RabbitMQContext

import java.nio.file.Paths
//
import mx.cinvestav.Declarations.{CacheTransaction, CommandIds, NodeContextV5,Payloads}
import mx.cinvestav.cache.cache.CachePolicy
import mx.cinvestav.commons.compression
import mx.cinvestav.commons.types.ObjectMetadata
import mx.cinvestav.server.Routes.UploadResponse
import mx.cinvestav.utils.v2.encoders._
//
import org.http4s.blaze.server.BlazeServerBuilder
import org.http4s.server.{AuthMiddleware, Router}
import org.http4s.{AuthedRoutes, HttpRoutes, Request, Response}
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.multipart.Multipart
import org.typelevel.ci._
//
import io.circe.generic.auto._
import io.circe.syntax._
//
import cats.nio.file.{Files => NIOFIles}
//
import java.util.UUID
import scala.concurrent.ExecutionContext.global
import concurrent.duration._
import language.postfixOps
import mx.cinvestav.commons.{status=>StatuX}

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
      req                  = authReq.req
      nodeId               = ctx.config.nodeId
      poolId               = ctx.config.poolId
      port                 = ctx.config.port
      storagePath          =  ctx.config.storagePath
      key                  = guid.toString
      headers              = req.headers
      compressionAlgorithm = headers.get(CIString("Compression-Algorithm")).map(_.head).map(_.value).map(_.toUpperCase()).getOrElse("")
      extension            = headers.get(CIString("File-Extension")).map(_.head).map(_.value).getOrElse("")
      ca                   = compression.fromString(compressionAlgorithm)
      userId               =  user.id
      bucketName           =  user.bucketName
      baseStr              =  s"$storagePath/$nodeId/$userId/$bucketName"
      basePath             =  Paths.get(baseStr)
      tempGuid  = guid
      //        = UUID.randomUUID().toString
      tempFolderPath       = basePath.resolve(s"temp-$tempGuid")
      tempFileStr          = basePath+s"/$tempGuid.$extension"
      tempFilePath         = Paths.get(tempFileStr)
      sourcePath           = basePath.resolve(key+"."+ca.extension)
      sourceExits          <- NIOFIles[IO].exists(sourcePath)
      tempExists           <- NIOFIles[IO].exists(tempFilePath)

      response2 <-  if(sourceExits && ca == compression.Default) for {
        response <- Ok()
      } yield response
      else if(sourceExits) for {
        _ <- IO.unit
        cache   = currentState.cache
        cacheX  = CachePolicy(cachePolicy)
        readRes <- cacheX.read(cache,key)
        _       <- ctx.state.update(_.copy(cache = readRes.newCache))
        res <- if(!tempExists)  for {
             _   <- NIOFIles[IO].createDirectories(tempFolderPath)
             res <- compression.decompress(ca,sourcePath.toString,tempFolderPath.toString ).value.flatMap {
               case Left(e) => ctx.logger.error(e.getMessage) *> InternalServerError()
               case Right(value) => for {
                 _    <- ctx.logger.debug(value.toString)
                 //            source = Paths.get(basePath.toString+s"/temp/$guid")
                 source  = tempFolderPath.resolve(guid.toString)
                 target  = Paths.get(tempFileStr)
                 _ <- ctx.logger.debug(s"MOVE_SOURCE $source")
                 _ <- ctx.logger.debug(s"MOVE_TARGET $target")
                 newPath <- NIOFIles[IO].move(source, target)
                 _ <- ctx.logger.debug(s"MOVE_NEW_PATH $newPath")
                 stream = Files[IO].readAll(newPath,8192)
                 clean = for {
                   _ <- ctx.logger.debug("CLEAN")
                   _ <- NIOFIles[IO].delete(tempFolderPath)
                   _ <- NIOFIles[IO].delete(tempFilePath)
//                   _ <- NIOFIles[IO].delete(target)
                   _ <- ctx.logger.debug("CLEAN_DONE")
                 } yield ()
                 _ <- (IO.sleep(30 seconds) *> clean).start
                 res    <- Ok(stream)
               } yield res
             }
        } yield res
        else for {
          _        <- ctx.logger.debug("EXISTS A UNCOMPRESSED FILE")
          stream_   = Files[IO].readAll(tempFilePath,8192)
          response <- Ok(stream_)
        } yield response

      } yield res
      else  for {
        _        <- ctx.logger.info(s"MISS $guid")
        response <- NotFound()
      } yield response
      _ <- ctx.logger.debug("______________________________________________________________________")
//      response     <- Ok("Download")
    } yield response2
    case authReq@POST -> Root / "upload" as user => for {
      timestamp            <- IO.realTime.map(_.toMillis)
      currentState         <- ctx.state.get
      queue                = currentState.queue
      status               = currentState.status
      req                  = authReq.req
      headers              = req.headers
      operationId          = headers.get(CIString("Operation-Id")).map(_.head).map(_.value).flatMap(_.toIntOption).getOrElse(0)
      _ <- if(status == StatuX.Up)  for {
        _ <- ctx.logger.debug(s"OPERATION_ID $operationId")
        _ <- ctx.state.update(_.copy(status = StatuX.Paused))
        _ <- Helpers.onUp(operationId,authReq)
      } yield ()
      else for {
        _    <- ctx.logger.debug("PAUSED")
        reqx = RequestX(operationId = operationId,authedRequest = authReq)
        _    <- queue.offer(reqx)
//        _<- fs2.Stream.from
      } yield ()
      response <- Ok("HERE!")
    } yield response


    case authReq@POST -> Root / "uploadv2" as user => for {
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
      _               <- NIOFIles[IO].createDirectories(basePath)
      //      _                    <- headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
      _                    <- parts.traverse{ part=>
        for {
          _                <- IO.unit
          cache            = currentState.cache
          _                <- part.headers.headers.traverse(header=>ctx.logger.debug(s"HEADER $header"))
          partHeaders      = part.headers
          //         Metadata
          guid             = partHeaders.get(key =ci"guid").map(_.head).map(_.value).map(UUID.fromString).getOrElse(UUID.randomUUID())
          contentType      = partHeaders.get(ci"Content-Type").map(_.head.value).getOrElse( "application/octet-stream")
          originalFilename = part.filename.getOrElse("default")
          originalName     = part.name.getOrElse("default")
          checksum         = ""
          ca               = compression.fromString(compressionAlgorithm)
          metadata         = ObjectMetadata(
            guid =  guid,
            size  = 0L,
            compression = ca.token,
            bucketName = bucketName,
            filename = originalFilename,
            name = originalName,
            locations = Nil,
            policies = Nil,
            checksum = checksum,
            userId = userId.toString,
            //              .getOrElse(UUID.randomUUID().toString),
            version = 1,
            timestamp = timestamp,
            contentType = contentType,
            extension = ""
          )
          //
          sinkPath         =  Paths.get(baseStr+s"/$guid")
          stream           = part.body
          writeFile        = stream.through(Files[IO].writeAll(path=sinkPath)).compile.drain
          //
          _                <- ctx.logger.debug("GUID "+guid.toString)
          putRes           <- cacheX.put(cache,guid.toString)
          maybeEvictedItem          = putRes.evicted
          _ <- maybeEvictedItem match {
            case Some(value) => for {
              //              _                <- NIOFIles[IO].delete(evictedPath)
              _            <- IO.unit
              evictedPath  = Paths.get(baseStr+s"/${value.key}.${ca.extension}")
              evictedExits <- NIOFIles[IO].exists(evictedPath)
              syncNode     = currentState.syncLB.balance(rounds = 1).head
              syncNodePub  = currentState.syncNodePubs(syncNode)
              _<- ctx.logger.debug(s"EVICTED_PATH $evictedPath")
              _<- ctx.logger.debug(s"EVICTED_EXISTS $evictedExits")
              _<- ctx.logger.debug(s"SYNC_NODE $syncNode")
//              _<- ctx.logger.debug(s"SYNC_NODE_PUB $syncNodePub")
              _ <- Helpers.sendPull(userId.toString,bucketName,value,syncNodePub)
              _   <- writeFile
              _   <- compression.compress(ca=ca,source = sinkPath.toString,baseStr)
                .value.flatMap {
                case Left(e) => ctx.logger.error(e.getMessage)
                case Right(value) => for {
                  _        <- ctx.logger.debug(value.toString)
                  _        <- ctx.state.update(_.copy(
                    status=StatuX.Up,
                    currentOperationId = None,
                    cache = putRes.newCache
                  ))
                  _        <- NIOFIles[IO].delete(sinkPath)
                  endTime  <- IO.realTime.map(_.toMillis)
                  duration = endTime-timestamp
                  _        <- ctx.logger.info(s"UPLOADV2 $guid $duration")
                } yield ()
              }.onError{ t=> ctx.logger.error(t.getMessage)}
            } yield ()
            case None =>  for {
              _                <- ctx.logger.debug("NO EVICTION")
              _                <- writeFile
              _        <- compression.compress(ca=ca,source = sinkPath.toString,baseStr)
                .value.flatMap {
                case Left(e) => ctx.logger.error(e.getMessage)
                case Right(value) => for {
                  _        <- ctx.logger.debug(value.toString)
                  //                FREE NODE
                  _        <- ctx.state.update(_.copy(status=StatuX.Up,currentOperationId = None))
                  _        <- NIOFIles[IO].delete(sinkPath)
                  endTime  <- IO.realTime.map(_.toMillis)
                  duration = endTime-timestamp
                  _        <- ctx.logger.info(s"UPLOADV2 $guid $duration")
                } yield ()
              }.onError{ t=> ctx.logger.error(t.getMessage)}
              _                <- ctx.state.update(_.copy(cache=putRes.newCache))
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
          cache        = currentState.cache
          elements     <- currentState.currentEntries.get
          values       <- elements.traverse(cache.lookup).map(_.sequence)
          res            <- values match {
            case Some(value) =>
              val result = (elements zip value ).toMap
              Ok(result.asJson)
            case None => Ok("NO STATS")
          }
//          res          <- Ok("Response")
        } yield (res)
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
