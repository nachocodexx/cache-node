package mx.cinvestav.cache

import cats.implicits._
import cats.effect._
import io.chrisdavenport.mules.MemoryCache
import mx.cinvestav.Declarations.{NodeContextV5, ProposedElement}

object cache{
  type CacheX = MemoryCache[IO,String,Int]
  case class EvictedItem(key:String,value:Int){
    def toProposeElement:ProposedElement = ProposedElement(key,value)
  }
  object EvictedItem {
    def empty:EvictedItem = EvictedItem("",0)
  }
  case class PutResponse(newCache:CacheX,evicted:Option[EvictedItem])
  case class EvictionResponse(newCache:CacheX,evictedItem: Option[EvictedItem])
  case class ReadResponse(newCache:CacheX,found:Boolean)
  case class WriteResponse(newCache:CacheX)
  sealed trait Policy {
    def putv2(cache: MemoryCache[IO,String,Int], key:String)(implicit ctx:NodeContextV5):IO[PutResponse]
    def put(cache: MemoryCache[IO,String,Int], key:String)(implicit ctx:NodeContextV5):IO[PutResponse]
    def write(cache: MemoryCache[IO,String,Int], key:String)(implicit ctx:NodeContextV5):IO[WriteResponse]
    def read(cache: MemoryCache[IO,String,Int], key:String)(implicit ctx:NodeContextV5):IO[ReadResponse]
    def eviction(cache:MemoryCache[IO,String,Int],remove:Boolean=false)(implicit ctx:NodeContextV5):IO[EvictionResponse]
    def evictionv2(cache:MemoryCache[IO,String,Int],remove:Boolean=false)(implicit ctx:NodeContextV5):IO[EvictionResponse]
    def remove(cache:CacheX,key:String)(implicit ctx:NodeContextV5):IO[CacheX]
  }
  class LFU() extends Policy {
    override def remove(cache:CacheX,key:String)(implicit ctx:NodeContextV5):IO[CacheX] = for {
      currentState <- ctx.state.get
      _            <- cache.delete(key)
      _            <- currentState.currentEntries.update(_.filter(_!=key))
    } yield cache
    override def write(cache:MemoryCache[IO,String,Int], key:String)(implicit ctx:NodeContextV5): IO[WriteResponse] = for {
      currentState <- ctx.state.get
      maybeValue   <- cache.lookup(key)
      //      _            <- currentState.currentEntries.update(_:+key)
      value        <- maybeValue match {
        case Some(value) =>
          (value+1).pure[IO]
        case None =>
          0.pure[IO]
      }
      _            <- cache.insert(key,value)
      response     = WriteResponse(newCache = cache)
    } yield response

    override def eviction(cache: MemoryCache[IO, String, Int],remove:Boolean=false)(implicit ctx: NodeContextV5): IO[EvictionResponse] = for {
      currentState <- ctx.state.get
      elements     <- currentState.currentEntries.get
      maybeValues  <- elements.traverse(cache.lookup).map(_.sequence).map(_.mapFilter(xs=>Option.when(xs.nonEmpty)(xs)))
//      _            <- ctx.logger.debug(maybeValues.map(_.mkString(",")).toString)
      x            <- maybeValues match {
        case Some(values) =>

          val sorted  = (elements zip values).sortBy(_._2)
          val evicted = (EvictedItem.apply _) tupled sorted.head
          for {
            _     <- if(remove) for {
              _      <- cache.delete(evicted.key)
              _      <- currentState.currentEntries.update(_.filter(_!=evicted.key))
            } yield ()
            else IO.unit
            result = EvictionResponse( cache,evicted.some  )
          } yield result
        case None => EvictionResponse(cache,evictedItem = None).pure[IO]
      }
    } yield x

    override def put(cache: MemoryCache[IO, String, Int], key: String)(implicit ctx: NodeContextV5): IO[PutResponse] = for {
      _            <- ctx.logger.info(s"PUT $key")
      currentState <- ctx.state.get
      elements     <- currentState.currentEntries.get
      cacheSize    = currentState.cacheSize
      res          <- if(elements.length == cacheSize) for {
        evictionResponse  <- eviction(cache,remove = true)
        writeResponse     <- write(evictionResponse.newCache,key)
        _                 <- currentState.currentEntries.update(_:+key)
      } yield evictionResponse.copy(newCache=writeResponse.newCache)
      else for {
        writeRes    <- write(cache,key)
        _           <- currentState.currentEntries.update(_:+key)
//        _           <- IO.unit
      } yield EvictionResponse(writeRes.newCache,evictedItem = None)
      response = PutResponse(newCache = res.newCache,evicted = res.evictedItem)
    } yield response

    override def read(cache: MemoryCache[IO, String, Int], key: String)(implicit ctx: NodeContextV5): IO[ReadResponse] = for {
      _          <- ctx.logger.info(s"GET $key")
      maybeValue <- cache.lookup(key)
      value      <- maybeValue match {
        case Some(value) =>
          val newValue = value+1
          newValue.pure[IO] <* ctx.logger.info(s"HIT $key $newValue")
        case None =>
          0.pure[IO] <* ctx.logger.info(s"MISS $key")
      }
      _         <- if(value==0) IO.unit else cache.insert(key,value)
      response = ReadResponse(newCache = cache,found= if(value==0) false else true)
//      response = if ( value ==0 )  ( cache,false) else (cache,true)
    } yield response

    override def putv2(cache: MemoryCache[IO, String, Int], key: String)(implicit ctx: NodeContextV5): IO[PutResponse] =  for {
      _            <- ctx.logger.info(s"PUT $key")
      currentState <- ctx.state.get
      elements     <- currentState.currentEntries.get
      cacheSize    = currentState.cacheSize
      res          <- if(elements.length == cacheSize) for {
        evictionResponse  <- eviction(cache)
      } yield evictionResponse
      else for {
        writeRes    <- write(cache,key)
        _           <- currentState.currentEntries.update(_:+key)
        //        _           <- IO.unit
      } yield EvictionResponse(writeRes.newCache,evictedItem = None)
      response = PutResponse(newCache = res.newCache,evicted = res.evictedItem)
    } yield response

    override def evictionv2(cache: MemoryCache[IO, String, Int], remove: Boolean)(implicit ctx: NodeContextV5): IO[EvictionResponse] = for {
      currentState <- ctx.state.get
      cacheSize    = ctx.config.cacheSize
      elements     <- currentState.currentEntries.get
      res <- if(elements.length <  cacheSize) IO.pure(EvictionResponse(cache,None))
      else for {
        maybeValues  <- elements.traverse(cache.lookup).map(_.sequence).map(_.mapFilter(xs=>Option.when(xs.nonEmpty)(xs)))
        _            <- ctx.logger.debug(maybeValues.map(_.mkString(",")).toString)
        x            <- maybeValues match {
          case Some(values) =>
            val sorted  = (elements zip values).sortBy(_._2)
            val evicted = (EvictedItem.apply _) tupled sorted.head
            for {
              _     <- if(remove) for {
                _      <- cache.delete(evicted.key)
                _      <- currentState.currentEntries.update(_.filter(_!=evicted.key))
              } yield ()
              else IO.unit
              result = EvictionResponse( cache,evicted.some  )
            } yield result
          case None => EvictionResponse(cache,evictedItem = None).pure[IO]
        }
      } yield x
    } yield res

  }

  class LRU() extends Policy {
    override def write(cache:MemoryCache[IO,String,Int], key:String)(implicit ctx:NodeContextV5): IO[WriteResponse] = for {
      currentState       <- ctx.state.get
      downloadCounter    = currentState.downloadCounter
      maybeValue         <- cache.lookup(key)
      _                  <- currentState.currentEntries.update(_:+key)
      value              <- maybeValue match {
        case Some(value) => for {
          newDownloadCounter <- ctx.state.updateAndGet(_.copy(downloadCounter = downloadCounter+1)).map(_.downloadCounter)
          _                  <- cache.insert(key,newDownloadCounter)
        }  yield (cache)
        case None => for {
          newDownloadCounter <- ctx.state.updateAndGet(_.copy(downloadCounter = downloadCounter+1)).map(_.downloadCounter)
          _   <- cache.insert(key,newDownloadCounter)
          res <- IO.pure(cache)
        } yield res
      }
      response = WriteResponse(newCache = value)
    } yield response

    override def eviction(cache: MemoryCache[IO, String, Int],remove:Boolean=false)(implicit ctx: NodeContextV5): IO[EvictionResponse] =  for {
      currentState       <- ctx.state.get
      elements           <- currentState.currentEntries.get
      //    _                  <- IO.println(elements)
      maybeValues        <- elements.traverse(cache.lookup)
        //      .flatTap(xs=>IO.println(xs.mkString(" // ")))
        .map(_.sequence)
      x                  <- maybeValues match {
        case Some(values) =>
          val sorted  = (elements zip values).sortBy(_._2)
          val evicted = sorted.head
          val evictedKey = evicted._1
          for {
            _      <- cache.delete(evictedKey)
            _      <- currentState.currentEntries.update(_.filter(_!=evictedKey))
            result = (cache,Some(evictedKey))
          } yield result
        case None =>
          (cache,Option.empty[String]).pure[IO]
      }
      response = EvictionResponse(newCache = x._1,evictedItem = None)
    } yield response

    override def put(cache: MemoryCache[IO, String, Int], key: String)(implicit ctx: NodeContextV5): IO[PutResponse] = for {
      currentState <- ctx.state.get
      elements     <- currentState.currentEntries.get
      cacheSize    = currentState.cacheSize
      //    _            <- IO.println(s"CACHE_SIZE $cacheSize / ${elements.length}")
      res          <- if(elements.length == cacheSize) for {
        evictionRes <- eviction(cache)
        writeRes  <- write(evictionRes.newCache,key)
        response = PutResponse(newCache = writeRes.newCache,evicted = evictionRes.evictedItem)
      } yield response
      else for {
        c0      <- write(cache,key)
        putRes = PutResponse(newCache = c0.newCache,evicted = None)
      } yield putRes
    } yield res

    override def read(cache: MemoryCache[IO, String, Int], key: String)(implicit ctx: NodeContextV5): IO[ReadResponse] = for {

      currentState       <- ctx.state.get
      downloadCounter    = currentState.downloadCounter
      maybeValue         <- cache.lookup(key)
      value              <- maybeValue match {
        case Some(value) => for {
          newDownloadCounter <- ctx.state.updateAndGet(_.copy(downloadCounter = downloadCounter+1)).map(_.downloadCounter)
          _ <- IO.println(s"FOUND $key $newDownloadCounter")
          _                  <- cache.insert(key,newDownloadCounter)
        }  yield ReadResponse(newCache = cache,found = true)
        case None => for {
          //        newDownloadCounter <- ctx.state.updateAndGet(_.copy(downloadCounter = downloadCounter+1)).map(_.downloadCounter)
          //        _   <- cache.insert(key,newDownloadCounter)
          res <-  ReadResponse(newCache = cache,found = false).pure[IO]
        } yield res
      }
//      response = ReadResponse(newCache = cache,found = true)
    } yield value

    override def putv2(cache: MemoryCache[IO, String, Int], key: String)(implicit ctx: NodeContextV5): IO[PutResponse] = ???

    override def evictionv2(cache: MemoryCache[IO, String, Int], remove: Boolean)(implicit ctx: NodeContextV5): IO[EvictionResponse] = ???

    override def remove(cache: CacheX, key: String)(implicit ctx: NodeContextV5): IO[CacheX] = ???
  }

  //sealed trait LRU extends Policy
  object CachePolicy {
    def apply(policy:String):Policy = policy match {
      case "LFU" => new LFU()
      case "LRU" => new LRU()
    }
  }
}

