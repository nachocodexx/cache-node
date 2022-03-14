package mx.cinvestav
import breeze.linalg.sum
import cats.implicits._
import cats.effect.{IO, Ref}
import mx.cinvestav.Declarations.IObject
import mx.cinvestav.commons.types.{HitCounterInfo, QueueTimes}
import mx.cinvestav.events.Events.onlyPuts

import scala.util.Try
//
import fs2.io.file.Files
import fs2.Stream
//
import com.dropbox.core.v2.files.FileMetadata
//
import mx.cinvestav.Declarations.{IObject, NodeContext, ObjectD, ObjectS}
import mx.cinvestav.clouds.Dropbox
import mx.cinvestav.commons.events.{Del, EventX, Push, Put}
import mx.cinvestav.events.Events
import mx.cinvestav.cache.CacheX
//
import retry.{RetryDetails, RetryPolicies, retryingOnAllErrors}
//
import java.nio.file.Paths
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.DeflaterOutputStream
import java.util.zip.InflaterOutputStream
//
import concurrent.duration._
//
import io.circe.syntax._
import io.circe.generic.auto._
//
import org.http4s._
//
import org.typelevel.ci.CIString
//
import scala.concurrent.duration._
import language.postfixOps
//
import mx.cinvestav.commons.events.EventXOps
import mx.cinvestav.commons.types.DumbObject
import mx.cinvestav.commons.replication.popularity.nextNumberOfAccess

object Helpers {



  def getHitInfo(nodeId:String,events:List[EventX],period:FiniteDuration) = {
    for {
      currentMonotinic            <- IO.monotonic.map(_.toNanos)
      uploads                     = EventXOps.onlyPuts(events=events)
      downloads                   = EventXOps.onlyGets(events=events)
      objectIds                   = Events.getObjectIds(events=events)
      dumbObjects                 = Events.getDumbObjects(events=events)
      counter                     = Events.getHitCounterByNodeV2(events=events)
      mx                          = Events.generateMatrixV2(events=events)
      currentIdleTime             = events.maxByOption(_.monotonicTimestamp).map(_.monotonicTimestamp).map(x=> currentMonotinic - x)
      puts                        = onlyPuts(events = events).map(_.asInstanceOf[Put])
      userIds                     = puts.map(_.userId).distinct
      xSum                        = sum(mx)
      x                           = if(xSum == 0.0) mx.toArray.toList.map(_=>0.0) else (mx/xSum).toArray.toList
      y                           = Events.getHitCounterByUser(events=events)
      normalizeCounter            = (objectIds zip x).toMap
      objectSizes                 = dumbObjects.map{
        case o@DumbObject(objectId, objectSize) =>
          (objectId -> objectSize)
      }.toMap
      nextNumberOfAccess          = Helpers.generateNextNumberOfAccessByObjectId(events=events)(period)
      //      SERVICE TIME
      _                   <- IO.unit
      uploadServiceTime   = EventXOps.getMeanAndMedianServiceTime(events=uploads)
      downloadServiceTime = EventXOps.getMeanAndMedianServiceTime(events=downloads)
      globalServiceTime   = EventXOps.getMeanAndMedianServiceTime(events=events)
      //      ARRIVAL TIME
      uploadArrivalTime   = EventXOps.getMeanAndMedianArrivalTime(events=uploads)
      downloadArrivalTime = EventXOps.getMeanAndMedianArrivalTime(events=downloads)
      globalArrivalTime   = EventXOps.getMeanAndMedianArrivalTime(events=events)
      //      INTERARRIVAL TIME
      uploadInterArrivalTime   = EventXOps.getMeanAndMedianInterArrivalTime(events=uploads)
      downloadInterArrivalTime = EventXOps.getMeanAndMedianInterArrivalTime(events=downloads)
      globalInterArrivalTime   = EventXOps.getMeanAndMedianInterArrivalTime(events=events)
      //      INTERARRIVAL TIME
      uploadWaitingTime   = EventXOps.getMeanAndMedianWaitingTime(events=uploads)
      downloadWaitingTime = EventXOps.getMeanAndMedianWaitingTime(events=downloads)
      globalWaitingTime   = EventXOps.getMeanAndMedianWaitingTime(events=events)
      ////      INTERARRIVAL TIME
      uploadIdleTime   = EventXOps.getMeanAndMedianIdleTimes(events=uploads)
      downloadIdleTime = EventXOps.getMeanAndMedianIdleTimes(events=downloads)
      globalIdleTime   = EventXOps.getMeanAndMedianIdleTimes(events=events)
      hitVec = HitCounterInfo(
        nodeId             = nodeId,
        objectIds          = objectIds,
        userIds            = userIds,
        hitCounter         = counter,
        normalizeCounter   = normalizeCounter,
        hitCounterByUser   = y,
        objectSizes        = objectSizes,
        nextNumberOfAccess = nextNumberOfAccess,
        uploadQueueTimes   = QueueTimes(
          meanServiceTime        = uploadServiceTime._1,
          medianServiceTime      = uploadServiceTime._2,
          meanArrivalTime        = uploadArrivalTime._2,
          medianArrivalTime      = uploadArrivalTime._2,
          meanInterarrivalTime   = uploadInterArrivalTime._1,
          medianInterarrivalTime = uploadInterArrivalTime._2,
          meanWaitingTime        = uploadWaitingTime._1,
          medianWaitingTime      = uploadWaitingTime._2,
          meanIdleTime           =  uploadIdleTime._1,
          medianIdleTime         = uploadIdleTime._2
        ).toSeconds,
        downloadQueueTimes = QueueTimes(
          meanServiceTime        = downloadServiceTime._1,
          medianServiceTime      = downloadServiceTime._2,
          meanArrivalTime        = downloadArrivalTime._1,
          medianArrivalTime      = downloadArrivalTime._2,
          meanInterarrivalTime   = downloadInterArrivalTime._1,
          medianInterarrivalTime = downloadInterArrivalTime._2,
          meanWaitingTime        = downloadWaitingTime._1,
          medianWaitingTime      = downloadWaitingTime._2,
          meanIdleTime           =  downloadIdleTime._1,
          medianIdleTime         = downloadIdleTime._2
        ).toSeconds,
        globalQueueTimes   = QueueTimes(
          meanServiceTime        = globalServiceTime._1 ,
          medianServiceTime      = globalServiceTime._2,
          meanArrivalTime        = globalArrivalTime._1,
          medianArrivalTime      = globalArrivalTime._2,
          meanInterarrivalTime   = globalInterArrivalTime._1,
          medianInterarrivalTime = globalInterArrivalTime._2,
          meanWaitingTime        = globalWaitingTime._1,
          medianWaitingTime      = globalWaitingTime._2,
          meanIdleTime           =  globalIdleTime._1,
          medianIdleTime         = globalIdleTime._2
        ).toSeconds,
        volumeByUser       = Events.calculateVolumeByUser(events=events),
        densityByUser      = Events.calculateDensityByUser(events=events),
        currentIdleTime    = currentIdleTime.map(x=>x.toDouble).getOrElse(0.0)/1000000000.0
      )
    } yield hitVec
  }
//  __________________________________________
  def  generateNextNumberOfAccessByObjectId(events:List[EventX])(period:FiniteDuration) = {
    val F       = EventXOps.getDumbObjects(events=events)
//    println("F: "+F)
     F.map{ f =>
      val y = Events.getDownloadsByIntervalByObjectId(objectId = f.objectId)(period)(events=events)
//       println("y: "+y)
      f.objectId -> nextNumberOfAccess(y)
    }.toMap
  }
//  ___________________________________________


  def uploadObj(
                 operationId:String,
                 objectId:String,
                 objectSize:Long,
                 bytesBuffer:Stream[IO,Byte],
                 objectExtension:String,
                 producerId:String,
                 waitingTime:Long = 0L
               )(implicit ctx:NodeContext)= {
    for {
      _               <- IO.unit
      serviceTimeStart <- IO.monotonic.map(_.toNanos)
      currentState    <- ctx.state.get
      currentEvents   = currentState.events
      newObject       <- if(!ctx.config.inMemory) {
        for {
          _    <- IO.unit
          meta = Map("objectSize"->objectSize.toString, "contentType" -> "", "extension" -> objectExtension)
          path = Paths.get(s"${ctx.config.storagePath}/$objectId")
          o    = ObjectD(guid=objectId,path =path,metadata=meta).asInstanceOf[IObject]
          _    <- bytesBuffer.through(Files[IO].writeAll(path)).compile.drain
        } yield o
      } else {
        for {
           bytes <- bytesBuffer.compile.to(Array)
           o     = ObjectS(
             guid     = objectId,
             bytes    = bytes,
             metadata = Map(
               "objectSize"->objectSize.toString,
               "contentType" -> "",
               "extension" -> objectExtension
             )
          ).asInstanceOf[IObject]
        } yield o
      }
      //        PUT TO CACHE
      evictedElement  <- IO.delay{CacheX.put(events = currentEvents,cacheSize = ctx.config.cacheSize,policy = ctx.config.cachePolicy)}
      now             <- IO.realTime.map(_.toNanos)
      putEndAtNanos   <- IO.monotonic.map(_.toNanos)
      _put            <- evictedElement match {
        case Some(evictedObjectId) => for {
          maybeEvictedObject <- currentState.cache.lookup(evictedObjectId)
          x                  <- maybeEvictedObject match {
            case Some(evictedObject) => for {
              _                    <- IO.unit
              evictedObjectBytes   <- evictedObject match {
                case ObjectD(_,path,_) => Files[IO].readAll(path,chunkSize = 8192).compile.to(Array)
                case ObjectS(_,bytes,_) => bytes.pure[IO]
              }
              evictedObjectSize    = evictedObjectBytes.length
//
              delete               = evictedObject match {
                case _:ObjectD => true
                case _:ObjectS => false
              }
//
              evictedContentType    = MediaType.unsafeParse(evictedObject.metadata.getOrElse("contentType","application/octet-stream"))
//
              _                     <- Helpers.pushToNextLevel(
                evictedObjectId = evictedObjectId,
                bytes = evictedObjectBytes,
                metadata = evictedObject.metadata,
                currentEvents = currentEvents,
                correlationId = operationId,
                delete = delete
              ).start
//
              deleteStartAtNanos     <- IO.monotonic.map(_.toNanos)
//              DELETE FROM CACHE
              _                      <- currentState.cache.delete(evictedObjectId)
              deleteEndAt            <- IO.realTime.map(_.toMillis)
              deleteEndAtNanos       <- IO.monotonic.map(_.toNanos)
              deleteServiceTimeNanos = deleteEndAtNanos - deleteStartAtNanos
              //                PUT NEW OBJECT IN CACHE
//              putStartAtNanos        <- IO.monotonic.map(_.toNanos)
              _                      <- currentState.cache.insert(objectId,newObject)
              putEndAt               <- IO.realTime.map(_.toNanos)
              putEndAtNanos          <- IO.monotonic.map(_.toNanos)
              putServiceTimeNanos    = putEndAtNanos - serviceTimeStart
              delEvent               = Del(
                serialNumber = 0 ,
                nodeId = ctx.config.nodeId,
                objectId = evictedObjectId,
                objectSize = evictedObjectSize,
                timestamp =deleteEndAt,
                serviceTimeNanos = deleteServiceTimeNanos,
                correlationId = operationId
              )
              _put                   = Put(
                  serialNumber     = 0,
                  nodeId           = ctx.config.nodeId,
                  objectId         = newObject.guid,
                  objectSize       = objectSize,
                  timestamp        = putEndAt,
                  serviceTimeNanos = putServiceTimeNanos,
                  correlationId    = operationId,
                  userId           = producerId,
                  serviceTimeStart = serviceTimeStart,
                  serviceTimeEnd   = putEndAtNanos,
                  replication      = true
                )
              _                      <- Events.saveEvents(List(delEvent,_put))
              //                      EVICTED
              _                      <- ctx.config.pool.sendEvicted(delEvent).start
//              _                      <- ctx.config.pool.sendPut(_put).start
          } yield _put
          case None =>
            for {
              _ <- ctx.logger.error("WARNING: OBJECT WAS NOT PRESENT IN THE CACHE.")
            } yield Put.empty
          }
        } yield x
        //               NO EVICTION
        case None => for {
          //             PUT NEW OBJECT
          _                   <- currentState.cache.insert(objectId,newObject)
          putServiceTimeNanos = putEndAtNanos - serviceTimeStart
          _put                = Put(
              serialNumber     = 0,
              nodeId           = ctx.config.nodeId,
              objectId         = newObject.guid,
              objectSize       = objectSize,
              timestamp        = now,
              serviceTimeNanos = putServiceTimeNanos,
              correlationId    = operationId,
              userId           = producerId,
              serviceTimeStart = serviceTimeStart,
              serviceTimeEnd   = putEndAtNanos

          )
          _                   <- Events.saveEvents(events =  List(_put))
//          _                   <- ctx.config.pool.sendPut(_put).start
        } yield _put
      }
      _ <- ctx.logger.info(s"PUT $objectId $objectSize $serviceTimeStart ${_put.serviceTimeEnd} ${_put.serviceTimeNanos} $waitingTime $operationId")

    } yield _put
  }

  def compress(in: Array[Byte]):IO[Array[Byte]] = {
    for {
      _    <- IO.unit
      out  = new ByteArrayOutputStream()
      defl = new DeflaterOutputStream(out)
      _    <- IO.delay(defl.write(in))
      _    <- IO.delay(defl.flush())
      _    <- IO.delay(defl.close())
    } yield out.toByteArray
  }

  def decompress(in: Array[Byte]): IO[Array[Byte]] = for {
    _    <- IO.unit
    out  = new ByteArrayOutputStream()
    infl = new InflaterOutputStream(out)
    _    <- IO.delay(infl.write(in))
    _    <- IO.delay(infl.flush())
    _    <- IO.delay(infl.close())
  } yield out.toByteArray

//  def pushToCloud(evictedObject:ObjectS,currentEvents:List[EventX],correlationId:String="")(implicit ctx:NodeContextV6): IO[Unit] = {
    def pushToNextLevel(
                     evictedObjectId:String,
                     bytes:Array[Byte],
                     metadata:Map[String,String],
                     currentEvents:List[EventX],
                     userId:String = "",
                     correlationId:String="",
                     delete:Boolean = false,
                   )(implicit ctx:NodeContext): IO[Unit] = {
    //  IO[List[EventX]] = {
    for {

      currentState  <- ctx.state.get
      alreadyPushed = if(ctx.config.cloudEnabled) Events.alreadyPushedToCloud(objectId = evictedObjectId,events = currentEvents) else false
      objectSize = bytes.length
      pushEvents             <- if(!alreadyPushed)
      {
        for {

          pushStartAtNanos       <- IO.monotonic.map(_.toNanos)
          evictedObjectExtension = metadata.getOrElse("extension","bin")
//          evictedObjectId        = evictedObjectId
          evictedObjectFilename  = evictedObjectId
//            s"${evictedObjectId}.${evictedObjectExtension}"
           x <- if(ctx.config.cloudEnabled) {
             for {
               _                      <- ctx.logger.debug(s"PUSH_NEXT_LEVEL $evictedObjectId CLOUD")
               fileExits              <- Dropbox.fileExists(currentState.dropboxClient)(filename = evictedObjectFilename)
               retryPolicy            = RetryPolicies.limitRetries[IO](10) join RetryPolicies.exponentialBackoff[IO](10 seconds)
               uploadIO  = Dropbox.uploadObject(currentState.dropboxClient)(
                 filename = evictedObjectFilename,
//                   s"$evictedObjectId.$evictedObjectExtension",
                 in = new ByteArrayInputStream(bytes)
               )
               sendToCloud = retryingOnAllErrors[FileMetadata](
                 policy = retryPolicy,
                 onError = (e:Throwable,d:RetryDetails) => ctx.errorLogger.error(e.getMessage)
               )(uploadIO)
               _         <- if(!fileExits) sendToCloud else
                 ctx.logger.info(s"ALREADY_PUSHED ${evictedObjectId} $objectSize CLOUD $correlationId")
             } yield ("","")
           }
          else {
             for {
               _ <- ctx.logger.debug(s"PUSH_NEX_LEVEL $evictedObjectId POOL")
               x <- ctx.config.cachePool.upload(
                 objectId = evictedObjectId,bytes = bytes,
                 userId=userId,
                 operationId = correlationId,
                 contentType = MediaType.forExtension(evictedObjectExtension).getOrElse(MediaType.application.`octet-stream`)
               ).onError{ e=>
                 ctx.errorLogger.error(e.getMessage)
               }
             } yield x
           }
          pushEndAt              <- IO.realTime.map(_.toMillis)
          pushEndAtNanos         <- IO.monotonic.map(_.toNanos)
          serviceTimePushNanos   =  pushEndAtNanos - pushStartAtNanos
          //
          pushEvent = Push(
            serialNumber     = 0,
            nodeId           = x._1,
            objectId         = evictedObjectId,
            objectSize       = objectSize,
            pushTo           = if(ctx.config.cloudEnabled) "cloud" else "cache-pool",
            timestamp        = pushEndAt,
            serviceTimeNanos = serviceTimePushNanos,
            correlationId    = correlationId,
            uri              = x._2
          )
          _ <- if(!ctx.config.cloudEnabled && x._1.isEmpty && x._2.isEmpty)
            ctx.logger.info(s"ALREADY_PUSHED ${evictedObjectId} $objectSize CACHE $correlationId")
          else for {
            _ <- Events.saveEvents(events = List(pushEvent))
            _   <- ctx.logger.info(s"PUSH ${evictedObjectId} $objectSize $serviceTimePushNanos $correlationId")
          } yield ()
        } yield ()
      }
      else ctx.logger.info(s"ALREADY_PUSHED ${evictedObjectId} $objectSize CLOUD $correlationId")
      _ <-  if(delete) Files[IO].delete(Paths.get(s"${ctx.config.storagePath}/$evictedObjectId")) else IO.unit
    } yield ()
//      pushEvents
  }


}
