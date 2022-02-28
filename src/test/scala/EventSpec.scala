import breeze.linalg.sum
import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.{Del, EventX, EventXOps, Get, Pull, Put, TransferredTemperature => SetDownloads}
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.Implicits._
import mx.cinvestav.Helpers
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import scala.concurrent.duration._
import language.postfixOps
class EventSpec  extends munit .CatsEffectSuite {



  test("Basic") {
    val basePut = Put(
      eventId = "event-0",
      serialNumber = 0,
      nodeId = "cache-0",
      objectId = "F0",
      objectSize = 1000,
      timestamp = 0,
      serviceTimeNanos = 0
    )
    val baseGet = Get(
      eventId = "event-1",
      serialNumber = 1,
      nodeId = "cache-0",
      objectId = "F0",
      objectSize = 1000,
      timestamp = 1,
      serviceTimeNanos = 0,
      monotonicTimestamp = 1,
      userId = "0"
    )
    val baseDel = Del(
      eventId = "event-2",
      serialNumber = 2,
      nodeId = "cache-0",
      objectId = "F0",
      objectSize = 1000,
      timestamp = 3,
      serviceTimeNanos = 0L
    )
    val basePull = Pull(
      eventId = "event-4",
      serialNumber = 4,
      nodeId = "cache-0",
      objectId = "F0",
      objectSize = 1000 ,
      pullFrom = "Dropbox",
      timestamp = 54,
      serviceTimeNanos = 0L
    )
    val baseSetDownloads = SetDownloads(
      eventId = "",
      serialNumber = 0,
      nodeId = "cache-0",
      objectId = "F1",
      counter = 0,
      timestamp = 0,
      serviceTimeNanos = 0L,userId="1"
    )

    val rawEvents = List(
      basePut,
      baseGet.copy(
          monotonicTimestamp = 23010974823201L
      ),
      baseGet.copy(
        monotonicTimestamp = 23015933029880L
      ),
      baseGet.copy(
        monotonicTimestamp = 23015933029880L,
        serialNumber = 3,
        eventId = "event-3"
      ),
      baseGet.copy(
        monotonicTimestamp = 23015933029880L,
        serialNumber = 4,
        eventId = "event-4"
      ),
      baseGet.copy(
        monotonicTimestamp = 23015933029880L,
        serialNumber = 4,
        eventId = "event-4"
      ),
      baseGet.copy(
        monotonicTimestamp = 23015933029880L,
        serialNumber = 5,
        eventId = "event-6"
      ),
      baseGet.copy(
        monotonicTimestamp = 23015933029880L,
        serialNumber = 7,
        eventId = "event-7"
      ),



    )
    val events = Events.relativeInterpretEvents(rawEvents)
//    println(events)
    val  x = Events.getDownloadsByInterval(period = 1 second)(events=events)
    val  y = Helpers.generateNextNumberOfAccessByObjectId(events=events)(1 second)
//        Events.getDownloadsByIntervalByObjectId("F0")(1 second)(events=events)
      println(x)
      println(y)
//    val evictedElement0 = Events.LFU(events = events,cacheSize = 3)
//    val evictedElement1 = Events.LRU(events = events,cacheSize = 3)
//    val counter = Events.getHitCounterByNodeV2(events=events)
//    val mx      = Events.generateMatrixV2(events=events)
//    val x= mx/sum(mx)
//    val y = Events.getHitCounterByUser(events=events)
//    println(evictedElement0)
//    println(evictedElement1)
//    println(counter)
//    println(mx)
//    println(x)
//    println(y)

//    val x= EventXOps.OrderOps.byTimestamp(EventXOps.onlyGets(events = events)).map(_.asInstanceOf[Get]).map(_.objectId).distinct
//    val y = x.last
//    println(x)
//    println(y)
//    LFU
//    val x = Events.getDownloadCounter(events = eventsCache0)
//    val minX = x.minBy(_._2)
//    println(x.asJson)
//    println(minX)
//      .reverse

//    println(eventsCache0.asJson)
//    val x = Events.getGetsOfObjects(eventsCache0)
//    val x = EventXOps.OrderOps.bySerialNumber(rawEvents)
//    println(x.asJson)

  }

//
//    val objectIdInitCounter = objectIds.map(x=>(x,0)).toMap
////    println(objectIdInitCounter.asJson)
////  objectId -> NonEmptyList[NodeId]
//    val objectIdToNodes = puts.map(_.asInstanceOf[Put]).map{ e=>
//      Map(e.objectId -> List(e.nodeId))
//    }.foldLeft(Map.empty[String,List[String]])(_|+|_)
////  ObjectId -> Int
//    val filteredGets = objectIdInitCounter |+| gets.map(_.asInstanceOf[Get]).collect{ e=>
//      Map(e.objectId->1)
//    }.foldLeft(Map.empty[String,Int])(_|+|_)
////  Total of downloads
//    val totalDownloads = filteredGets.values.sum
////    println(s"TOTAL_DOWNLOADS $totalDownloads")
////  Get temperature
//    val temperatureMap = filteredGets.map(x=>(x._1,x._2.toDouble/totalDownloads.toDouble))
////    println(s"TEMPERATURE_MAP ${temperatureMap.asJson}")
////  threshold
//    val thresholdFilter = 0.10
////
//    val replicateObjects = temperatureMap.filter(_._2 > thresholdFilter)
////
//    val candidatedNodes  = replicateObjects.map{
//      case (objectId, temperature) =>
//        val replicateNodes = objectIdToNodes.get(objectId)
//        replicateNodes.map(xs=>nodeIds.toSet.diff(xs.toSet)).map(_.toList)
//        .map(x=>Map(objectId->x))
//    }.toList.sequence.map(_.foldLeft(Map.empty[String,List[String]])(_|+|_))
////    println(candidatedNodes)
////    println(filteredGets.asJson)
////    println(temperatureMap.asJson)
////    println(fileAndCacheNodes.asJson)
////    println(replicateObjects.asJson)
//    val response = Json.obj(
////  "filtered_events" -> filteredEvents.asJson,
//          "nodeId" -> "cache-0".asJson,
//          "objects" -> objectIds.asJson,
//          "hit_counter"-> filteredGets.asJson
//    )
//    println(response)
//  }

}
