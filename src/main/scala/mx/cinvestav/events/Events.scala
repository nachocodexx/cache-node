package mx.cinvestav.events

import cats.implicits._
import mx.cinvestav.commons.events.EventXOps.OrderOps
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, EventXOps, Evicted, Get, Pull, Push, Put, RemovedNode, SetDownloads, Uploaded}

object Events {

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
          case  _:Uploaded | _:Downloaded | _: AddedNode | _: RemovedNode | _: Get | _:Put | _:Pull | _:Evicted | _:Push | _:SetDownloads =>
            _events :+  e
//          case _ => _events
        }
    }


  def getHitCounter(events:List[EventX]): Map[String, Int] = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0).toMap
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


  def LFU(events:List[EventX],cacheSize:Int = 3): Option[String] = {
      val initObjectIdCounter = Events.getObjectIds(events = events)
      val x =  initObjectIdCounter.map(x=>x->0).toMap|+|Events.getHitCounter(events = events)
//      println(x,x.size)
      if(x.size == cacheSize)
        x.minByOption(_._2).map(_._1)
      else None
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
