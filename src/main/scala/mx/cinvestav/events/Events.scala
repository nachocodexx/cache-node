package mx.cinvestav.events

import cats.implicits._
import mx.cinvestav.commons.events.EventXOps.OrderOps
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, EventXOps, Evicted, Get, Pull, Push, Put, RemovedNode, Uploaded}

object Events {

  def alreadyPushedToCloud(objectId:String,events:List[EventX]): Boolean = events.filter {
    case _: Push => true
    case _ => false
  }.map(_.asInstanceOf[Push])
    .exists(_.objectId == objectId)

  def getObjectIds(events:List[EventX]): List[String] = EventXOps.onlyPuts(events = events).map(_.asInstanceOf[Put]).map(_.objectId).distinct




  def getDownloadCounter(events:List[EventX]) = {
    EventXOps.onlyGets(events = events)
      .map(_.asInstanceOf[Get])
      .map{ get=>
        Map(get.objectId -> 1)
      }.foldLeft(Map.empty[String,Int])(_ |+| _)
  }

  def getPullOfObjects(events: List[EventX]) = {
    EventXOps.onlyGets(events = events)
      .map(_.asInstanceOf[Pull])
      .map{ pull=>
        Map(pull.objectId -> 1)
      }.foldLeft(Map.empty[String,Int])(_ |+| _)
  }


  def relativeInterpretEvents(events:List[EventX]):List[EventX] =
    OrderOps.byTimestamp(events).reverse.foldLeft(List.empty[EventX]){
      case (_events,e)=>
        e match {
          case _@Del(_, _, _, objectId,_,timestamp,_,_) =>
            _events.filterNot {
              case _del:Del => _del.objectId == objectId && _del.timestamp < timestamp
              case _get:Get => _get.objectId == objectId && _get.timestamp < timestamp
              case _put:Put => _put.objectId == objectId && _put.timestamp < timestamp
              case _ => false
//              case _ => false
            }
          case  _:Uploaded | _:Downloaded | _: AddedNode | _: RemovedNode | _: Get | _:Put | _:Pull | _:Evicted | _:Push =>
            _events :+  e
//          case _ => _events
        }
    }


  def LFU(events:List[EventX],cacheSize:Int = 3) = {
      val x = Events.getDownloadCounter(events = events)
      if(x.size == cacheSize)
        x.minByOption(_._2).map(_._1)
      else None
  }
  def LRU(events:List[EventX],cacheSize:Int=3) =  {
    val x= EventXOps.OrderOps.byTimestamp(EventXOps.onlyGets(events = events)).map(_.asInstanceOf[Get]).map(_.objectId).distinct
    if(x.length == cacheSize)
      x.lastOption
    else
      None
  }
}
