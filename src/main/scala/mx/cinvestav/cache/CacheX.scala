package mx.cinvestav.cache
import cats.Monad
import cats.data.Chain
import cats.implicits._
import cats.effect._
// A cache is just fast storage.
object CacheX {
  case class EvictedItem[A](key:String,value:A)
  trait CacheItem[A]{
    def value:A
    def counter:Int
  }
  case class LRUItem[A](value:A,counter:Int) extends CacheItem[A]
  case class LFUItem[A](value:A,counter:Int) extends CacheItem[A]
//
  trait CacheResponse
  case class PutResponse[A](evictedItem:Option[EvictedItem[A]]) extends CacheResponse
  case class GetResponse[A](key:String,item:Option[CacheItem[A]]) extends CacheResponse

  trait ICache[F[_],A] {
    def put(key:String,value:A):F[PutResponse[A]]
    def get(key:String):F[GetResponse[A]]
    def getAll:F[List[GetResponse[A]]]
  }


/*  LRU(Least Recently Used) ca
    - Space                          | O(n)
    - Get least recently used item  | O(1)
    - Access item                  | O(1)
 */
  object LRUItem {
    def unit[A](value:A) = LRUItem(value=value,counter=0)
  }
  class LRU[F[_],A] private (size:Int,var map:Map[String, LRUItem[A]],var globalCounter:Int=0)(implicit F:Monad[F]) extends ICache [F,A]{

    override def put(key: String, value: A): F[PutResponse[A]] = {
//     EVICTION POLICY
      if(size == this.map.size){
        val sorted = this.map.toList.sortBy(_._2.counter)
//        println(sorted)
        val lowerItem = sorted.headOption
        lowerItem match {
          case Some((k,lruItem)) =>
            val evictedItem = EvictedItem(k,lruItem.value)
            this.map        = this.map.removed(k)
            this.map        = this.map + (key-> LRUItem.unit(value))
            F.pure(PutResponse( evictedItem.some))
          case None =>
            F.pure(PutResponse(None))
        }
      }
      else {
        val newElement = key -> LRUItem.unit(value)
        this.map       = this.map +  newElement
        F.pure(PutResponse(None))
      }
    }
//
    override def get(key: String): F[ GetResponse[A] ] = {
      this.map.get(key) match {
        case Some(element) =>
          this.map = this.map.updated(key,element.copy(counter = globalCounter+1))
          this.globalCounter+=1
          F.pure(GetResponse(key,element.some ))
        case None =>
          F.pure(GetResponse(key,None))
      }
    }
//

    override def getAll: F[List[GetResponse[A]]] = this.map.map{
      case (key, value) => GetResponse(key,value.some)
    }.toList.pure[F]
  }


  object LRU {
    def apply[F[_]:Monad,A](size:Int = 3) = new LRU[F,A](
      size = size,
      map = Map.empty[String,LRUItem[A]]
    )
  }
//

  object LFUItem {
    def unit[A](value:A) = LFUItem(value=value,counter=0)
  }
  class LFU[F[_],A] private (size:Int,var map:Map[String, LFUItem[A]])(implicit F:Monad[F]) extends ICache [F,A]{

    override def put(key: String, value: A): F[PutResponse[A]] = {
      //     EVICTION POLICY
      if(size == this.map.size){
        val sorted = this.map.toList.sortBy(_._2.counter)
        println(sorted)
        val lowerItem = sorted.headOption
        println(lowerItem)

        lowerItem match {
          case Some((k,lfuItem)) =>
            val evictedItem = EvictedItem(key=k,value=lfuItem.value)
            this.map        = this.map.removed(k)
            this.map        = this.map + (key-> LFUItem.unit(value))
            F.pure(PutResponse( evictedItem.some))
          case None =>
            F.pure(PutResponse(None))
        }
      }
      else {
        val newElement = key -> LFUItem.unit(value)
        this.map       = this.map +  newElement
        F.pure(PutResponse(None))
      }
    }
    //
    override def get(key: String): F[GetResponse[A]] = {
      this.map.get(key) match {
        case Some(element) =>
          this.map = this.map.updated(key,element.copy(counter = element.counter+1))
          F.pure(GetResponse(key,element.some ))
        case None =>
          F.pure(GetResponse(key,None))
      }
    }
    //

    override def getAll: F[List[GetResponse[A]]] = this.map.map{
      case (key, value) => GetResponse(key,value.some)
    }.toList.pure[F]
  }


  object LFU {
    def apply[F[_]:Monad,A](size:Int = 3) = new LFU[F,A](
      size = size,
      map = Map.empty[String,LFUItem[A]]
    )
  }



}
