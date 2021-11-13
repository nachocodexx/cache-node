import cats.implicits._
import cats.effect._
import mx.cinvestav.commons.events.{Del, EventX, EventXOps, Get, Pull, Put, TransferredTemperature=>SetDownloads}
import mx.cinvestav.events.Events
import mx.cinvestav.Declarations.Implicits._
//
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
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
      serviceTimeNanos = 0
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
      serviceTimeNanos = 0L
    )

    val rawEvents = List(
        basePut,
        baseGet.copy(objectId = "F0"),
        baseGet.copy(objectId = "F0"),
        basePut.copy(objectId = "F1"),
        baseDel.copy(objectId = "F1"),
        basePut.copy(objectId = "F2"),

      //        basePut.copy(objectId = "F2"),
//        baseGet.copy(objectId = "F1"),
      //        basePut.copy(objectId = "F2"),
      //        baseGet,
//        baseDel,
//        basePut.copy(objectId = "F1"),
//        baseGet.copy(objectId = "F1"),
//        baseGet.copy(objectId = "F1"),
//        basePull,
//        basePut.copy(timestamp = 10),
//        baseGet.copy(timestamp = 11),
//        baseGet.copy(timestamp = 12),
//        baseGet.copy(timestamp = 13),
//        basePut.copy(objectId = "F2",timestamp = 14),
//        baseGet.copy(objectId = "F2",timestamp = 15),
//        baseSetDownloads.copy(counter = 10),
//        baseSetDownloads.copy(counter = 5,objectId = "F2"),
//        baseGet.copy(objectId = "F1",timestamp = 16),

      //        baseGet.copy(objectId = "F2",timestamp = 16),
//        baseGet.copy(objectId = "F2",timestamp = 17),

    )
    val events = Events.relativeInterpretEvents(rawEvents)
//    val x      = Events.getObjectIds(events = events)
//    println(x)
//    PUT
    val evictedElement0 = Events.LFU(events = events,cacheSize = 2)
    val evictedElement1 = Events.LRU(events = events,cacheSize = 2)
    println(evictedElement0)
    println(evictedElement1)

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
