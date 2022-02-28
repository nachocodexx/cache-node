package mx.cinvestav.events

import breeze.linalg.{*, DenseMatrix, DenseVector, sum}
import cats.effect.IO
import cats.implicits._
import mx.cinvestav.Declarations.NodeContext
import mx.cinvestav.commons.events.EventXOps.{OrderOps, sequentialMonotonic}
import mx.cinvestav.commons.events.{AddedNode, Del, Downloaded, EventX, EventXOps, Evicted, Get, Pull, Push, Put, RemovedNode, Uploaded, TransferredTemperature => SetDownloads}
import mx.cinvestav.commons.types.DumbObject
import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import language.postfixOps

object Events {
//
  def calculateVolumeByUser(events:List[EventX]): Map[String, Double] = {
    val puts              = onlyPuts(events=events).map(_.asInstanceOf[Put])
    val counterByObjectId = getCounterByObjecId(events=events)
    val dumbObjectsByUser = puts.groupBy(_.userId).map{
      case (userId,fs) => userId -> fs.map(x=>DumbObject(x.objectId,x.objectSize))
    }
    dumbObjectsByUser.map {
      case (userId, objects)=>
        userId -> objects.map(o=>counterByObjectId.getOrElse(o.objectId,0) * o.objectSize).sum.toDouble/1048576.0
    }
  }
//
  def calculateDensityByUser(events:List[EventX]): Map[String, Double] = {
    val puts               = onlyPuts(events=events).map(_.asInstanceOf[Put])
    val dumbObjectsByUser = puts.groupBy(_.userId).map{
      case (userId,fs) => userId -> fs.map(x=>x.objectId)
    }
    val getsByObjectsIdMap = getsByObjecId(events=events)
    dumbObjectsByUser map {
      case (userId, os) =>
        val gets = os.flatMap(objectId => getsByObjectsIdMap.getOrElse(objectId,List.empty[Get])).map(_.asInstanceOf[EventX])
        val ats  = EventXOps.getMeanInterArrivalTime(events=gets)
        userId -> ats/1000000000.0
    }
  }

  def getsByObjecId(events:List[EventX]): Map[String, List[Get]] = {
    val gets = onlyGets(events=events).map(_.asInstanceOf[Get])
    gets.groupBy(_.objectId).map{
      case (objectId, gs) => objectId -> gs
    }
  }

  def getCounterByObjecId(events:List[EventX]): Map[String, Int] = {
    val gets = onlyGets(events=events).map(_.asInstanceOf[Get])
    gets.groupBy(_.objectId).map{
      case (objectId, gs) => objectId -> gs.length
    }
  }
// _____________________________________________________________________________________________________________
  def getDownloadsByIntervalByObjectId(objectId:String)(period:FiniteDuration)(events:List[EventX]) = {
    val es = onlyGets(events=events).map(_.asInstanceOf[Get]).filter(_.objectId == objectId).map(_.asInstanceOf[EventX])
    Events.getDownloadsByInterval(period)(events=es).map(_.length)
  }
// _____________________________________________________________________________________________________________
  def getDownloadsByInterval(period:FiniteDuration)(events:List[EventX]): List[List[EventX]] = {
    val downloads         = onlyGets(events = events).sortBy(_.monotonicTimestamp)
    val maybeMinMonotonicTimestamp  = downloads.minByOption(_.monotonicTimestamp).map(_.monotonicTimestamp)
    maybeMinMonotonicTimestamp match {
      case Some(minMonotonicTimestamp) =>
        @tailrec
        def inner(es:List[List[EventX]], initT:FiniteDuration, i:Int=0):List[List[EventX]]= {
          val efs = es.flatten
          val L = efs.length
          if(L == downloads.length) es
          else {
            val newEs = downloads.toSet.diff(efs.toSet).toList
            if(newEs.isEmpty) es
            else {
              val newI  = i + 1
              val newT  =  (initT.toNanos+ period.toNanos).nanos
              inner(es = es:+newEs.filter(_.monotonicTimestamp <= initT.toNanos) , initT = newT ,i = newI)
            }
          }
        }
        inner(es= Nil,initT = (minMonotonicTimestamp + period.toNanos).nanos, i = 1)
      case None => Nil
    }
  }
// _____________________________________________________________________________________________________________
  def getHitCounterByNodeV2(events:List[EventX], windowTime:Long=0): Map[String, Double] = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0.0).toMap
    val filtered =  events.filter{
      case e@(_:Get | _:SetDownloads) => e.monotonicTimestamp > windowTime
      case _ => false
    }.foldLeft(List.empty[EventX]){
      case (_events,currentEvent)=> currentEvent match {
        case st:SetDownloads => _events.filterNot{
          case d:Get =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
          case d:SetDownloads =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
          case _=> false
        } :+ st
        case d:Get => _events :+ d
      }
    }
      .map{
      case d:Get => Map(d.objectId -> 1.0)
      case st: SetDownloads => Map(st.objectId -> st.counter.toDouble)
    }.foldLeft(Map.empty[String,Double])(_ |+| _)
    (filtered|+|downloadObjectInitCounter).toList.sortBy(_._1).toMap
  }
//

  def getHitCounterByUser(events:List[EventX], windowTime:Long=0): Map[String,Map[String, Double]] = {
    val objectsIds = getObjectIds(events = events)
    val puts       = onlyPuts(events = events).map(_.asInstanceOf[Put])
    val xPuts      = puts.map(x=>(x.objectId->x.userId)).toMap

    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0.0).toMap
    val filtered =  events.filter{
      case e@(_:Get | _:SetDownloads) => e.monotonicTimestamp > windowTime
      case _ => false
    }.foldLeft(List.empty[EventX]){
      case (_events,currentEvent)=> currentEvent match {
        case st:SetDownloads => _events.filterNot{
          case d:Get =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
          case d:SetDownloads =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
          case _=> false
        } :+ st
        case d:Get => _events :+ d
      }
    }
      .map{
        case d:Get =>
          val userId = xPuts(d.objectId)
          Map(userId -> Map(d.objectId -> 1.0))
        case st: SetDownloads =>
          val userId = xPuts(st.objectId)
          Map(userId -> Map(st.objectId -> st.counter.toDouble) )
      }.foldLeft(Map.empty[String,Map[String,Double]])(_ |+| _)
    (filtered).toList.sortBy(_._1).toMap
  }
//  Deprecated
  def getHitCounterByNode(events:List[EventX], windowTime:Long=0): Map[String, Map[String, Int]] = {
    val objectsIds                = getObjectIds(events = events)
    val downloadObjectInitCounter = objectsIds.map(x=> x -> 0).toMap
    val filtered =  events.filter{
      case e@(_:Get | _:SetDownloads) => e.monotonicTimestamp > windowTime
      case _ => false
    }.foldLeft(List.empty[EventX]){
      case (_events,currentEvent)=> currentEvent match {
        case st:SetDownloads => _events.filterNot{
          case d:Get =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
          case d:SetDownloads =>d.nodeId ==st.nodeId && d.timestamp < st.timestamp && d.objectId == st.objectId
          case _=> false
        } :+ st
        case d:Get => _events :+ d
      }
    }.map{
      case d:Get => Map(d.nodeId -> Map(d.objectId -> 1)  )
      case st: SetDownloads => Map(st.nodeId -> Map(st.objectId -> st.counter)  )
    }.foldLeft(Map.empty[String,Map[String,Int]  ])(_ |+| _)
      .map{
        case (objectId, value) => (objectId, downloadObjectInitCounter|+|value )
      }
    filtered.toList.sortBy(_._1).toMap
  }
//


  def generateMatrix(events:List[EventX]): DenseMatrix[Double] = {
    val hitCounter = ListMap(
      getHitCounterByNode(events = events).toList.sortBy(_._1):_*
    ).map{
      case (nodeId, counter) => nodeId-> ListMap(counter.toList.sortBy(_._1):_*)
    }
    //      .toMap
    //    println(s"HIT_COUNTER ${hitCounter}")
    val vectors = hitCounter
      .toList.sortBy(_._1)
      .map{
        case (nodeId, objectDownloadCounter) =>
          val values = objectDownloadCounter.values.toArray.map(_.toDouble)
          val vec = DenseVector(values:_*)
          vec
      }.toList
    DenseMatrix(vectors.map(_.toArray):_*)
  }

  def generateMatrixV2(events:List[EventX], windowTime:Long=0): DenseVector[Double] = {
    val hitCounter = ListMap(
      getHitCounterByNodeV2(events = events,windowTime = windowTime).toList.sortBy(_._1):_*
    )
    DenseVector.apply(hitCounter.values.toArray)
//    ()
//      .map{
//      case (objectId, counter) =>
//      case (nodeId, counter) => nodeId->
//        ListMap(counter.toList.sortBy(_._1):_*)
//    }
    //      .toMap
    //    println(s"HIT_COUNTER ${hitCounter}")
//    val vectors = hitCounter
//      .toList.sortBy(_._1)
//      .map{
//        case (objectId,counter)=>

//        case (nodeId, objectDownloadCounter) =>
//          val values = objectDownloadCounter.values.toArray.map(_.toDouble)
//          val vec = DenseVector(values:_*)
//          vec
//      }
//    DenseMatrix(vectors.map(_.toArray):_*)
  }

//  def generateReplicaUtilization(events:List[EventX]) = {
//    val x                = Events.generateMatrixV2(events = events,windowTime = 0)
//    val sumA             = (sum(x(::,*)).t).mapValues(x=>if(x==0) 1 else x)
//    val xx = x(*,::) / sumA
//    xx
//  }
//  def generateTemperatureMatrix(events:List[EventX]): DenseMatrix[Double] =  {
//    val x = Events.generateMatrix(events=events)
//    val dividend = sum(x(*,::))
//    x(::,*)  / dividend
//  }
//  def generateTemperatureMatrixV2(events:List[EventX],windowTime:Long): DenseMatrix[Double] =  {
//    val x = Events.generateMatrixV2(events=events,windowTime = windowTime)
//    val dividend = sum(x(*,::)).mapValues(x=>if(x==0) 1 else x)
//    x(::,*)  / dividend
//  }
//  def generateTemperatureMatrixV3(events:List[EventX],windowTime:Long): DenseMatrix[Double] =  {
//    val x = Events.generateMatrixV2(events=events,windowTime = windowTime)
//    x
//    //    val dividend = sum(x(*,::))
//    //    x(::,*)
//  }
//

//
  def getProducerIdByObjectId(objectId:String,events:List[EventX]): Option[String] = {
    val puts = onlyPuts(events=events).map(_.asInstanceOf[Put])
    puts.find(_.objectId == objectId).map(_.userId)
  }
  def onlyPuts(events:List[EventX]) = events.filter{
    case _:Put => true
    case _ => false
  }
  def onlyGets(events:List[EventX]) = events.filter{
    case _:Get => true
    case _ => false
  }

  def nextLevelUri(events:List[EventX],objectId:String) = {
    events.filter{
      case _:Push => true
      case _=> false
    }.map(_.asInstanceOf[Push]).filter(_.objectId==objectId)
      .maxByOption(_.monotonicTimestamp)
      .map(_.uri)
  }

  def saveEvents(events:List[EventX])(implicit ctx:NodeContext): IO[Unit] = for {
    currentState     <- ctx.state.get
    currentEvents    = currentState.events
    lastSerialNumber = currentEvents.length
    transformedEvents <- sequentialMonotonic(lastSerialNumber,events=events)
    _                <- ctx.state.update{ s=>
      s.copy(events =  s.events ++ transformedEvents )
    }
  } yield()

  //

//  def alreadyPushedToCloud(objectId:String,events:List[EventX]): Boolean = events.filter {
//    case _: Push => true
//    case _ => false
//  }.map(_.asInstanceOf[Push])
//    .exists(_.objectId == objectId)
  def alreadyPushedToCloud(objectId:String,events:List[EventX]): Boolean = events.filter {
    case _: Push => true
    case _ => false
  }.map(_.asInstanceOf[Push])
    .exists(_.objectId == objectId)


  def getDumbObjects(events:List[EventX]): List[DumbObject] =
    Events.onlyPuts(events = events).map(_.asInstanceOf[Put]).map{
    x=> DumbObject(x.objectId,x.objectSize)
  }.distinctBy(_.objectId)
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

//    println(x.length)
    if(x.length == cacheSize)
      x.lastOption
    else
      None
  }
}
