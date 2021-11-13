package mx.cinvestav.events

import cats.effect.IO
import cats.implicits._
import mx.cinvestav.Declarations.NodeContextV6
import mx.cinvestav.commons.events.EventXOps.{OrderOps, sequentialMonotonic}
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, EventXOps, Evicted, Get, Pull, Push, Put, RemovedNode, Uploaded, TransferredTemperature => SetDownloads}

object Events {

  def saveEvents(events:List[EventX])(implicit ctx:NodeContextV6): IO[Unit] = for {
    currentState     <- ctx.state.get
    currentEvents    = currentState.events
    lastSerialNumber = currentEvents.length
    transformedEvents <- sequentialMonotonic(lastSerialNumber,events=events)
    _                <- ctx.state.update{ s=>
      s.copy(events =  s.events ++ transformedEvents )
    }
  } yield()

  //
  def alreadyPushedToCloud(objectId:String,events:List[EventX]): Boolean = events.filter {
    case _: Push => true
    case _ => false
  }.map(_.asInstanceOf[Push])
    .exists(_.objectId == objectId)

  def getObjectIds(events:List[EventX]): List[String] =
  EventXOps.onlyPuts(events = events)
     .map(_.asInstanceOf[Put])
    .map(_.objectId).distinct




  def getDownloadCounter(events:List[EventX]) = {
    EventXOps.onlyGets(events = events)
      .map(_.asInstanceOf[Get])
      .map{ get=>
        Map(get.objectId -> 1)
      }.foldLeft(Map.empty[String,Int])(_ |+| _)
  }

  def getPullOfObjects(events: List[EventX]): Map[String, Int] = {
    EventXOps.onlyGets(events = events)
      .map(_.asInstanceOf[Pull])
      .map{ pull=>
        Map(pull.objectId -> 1)
      }.foldLeft(Map.empty[String,Int])(_ |+| _)
  }



  def relativeInterpretEventsMonotonic(events:List[EventX]):List[EventX] =
    events.sortBy(_.monotonicTimestamp).reverse.foldLeft(List.empty[EventX]){
      case (_events,e)=>
        e match {
          case del:Del =>
            _events.filterNot {
              case _del:Del => _del.objectId == del.objectId && _del.monotonicTimestamp < del.monotonicTimestamp
              case _get:Get => _get.objectId == del.objectId && _get.monotonicTimestamp < del.monotonicTimestamp
              case _put:Put => _put.objectId == del.objectId && _put.monotonicTimestamp < del.monotonicTimestamp
              case _ => false
            }
          case  _ => _events :+  e
        }
    }
  def relativeInterpretEvents(events:List[EventX]):List[EventX] =
    OrderOps.byTimestamp(events).reverse.foldLeft(List.empty[EventX]){
      case (_events,e)=>
        e match {
          case del:Del =>
            _events.filterNot {
              case _del:Del => _del.objectId == del.objectId && _del.timestamp < del.timestamp
              case _get:Get => _get.objectId == del.objectId && _get.timestamp < del.timestamp
              case _put:Put => _put.objectId == del.objectId && _put.timestamp < del.timestamp
              case _ => false
//              case _ => false
            }
          case  _ => _events :+  e
//          case _ => _events
        }
    }


  def getHitCounter(events:List[EventX]): Map[String, Int] = {
//    val objectsIds                = getObjectIds(events = events)
//    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0).toMap
    val filtered =  events.filter{
      case _:Get | _:SetDownloads => true
      case _ => false
    }.foldLeft(List.empty[EventX]){
      case (_events,currentEvent)=> currentEvent match {
        case st:SetDownloads => _events.filterNot{
          case d:Get =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
          case _=> false
        } :+ st
        case d:Get => _events :+ d
      }
    }.map{
      case d:Get =>Map(d.objectId -> 1)
      case st: SetDownloads => Map(st.objectId -> st.counter)
    }.foldLeft(Map.empty[String,Int])( _ |+| _)
//      .foldLeft(Map.empty[String,Int])(_ |+| _)
//      .map{
//        case (objectId, value) => (objectId, downloadObjectInitCounter|+|value )
//      }
    filtered
  }
//
//  __________________________________________________________________
  def LFU(events:List[EventX],cacheSize:Int = 3): Option[String] = {
      val initObjectIdCounter = Events.getObjectIds(events = events)
      val x =  initObjectIdCounter.map(x=>x->0).toMap|+|Events.getHitCounter(events = events)
      println(x,x.size)
      if(x.size < cacheSize) None
      else x.minByOption(_._2).map(_._1)
  }
  def LRU(events:List[EventX],cacheSize:Int=3): Option[String] =  {
//    val x= EventXOps.OrderOps.byTimestamp(EventXOps.onlyGets(events = events)).map(_.asInstanceOf[Get]).map(_.objectId).distinct
    val x= EventXOps.OrderOps.byTimestamp(events = events)
      .filter {
        case _:Get | _:Put => true
        case _=> false
      }
      .map{
        case g:Get => g.objectId
        case p:Put => p.objectId
      }
      .distinct

    if(x.length == cacheSize)
      x.lastOption
    else
      None
  }
}
