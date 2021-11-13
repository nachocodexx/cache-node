package mx.cinvestav.config

import cats.data.NonEmptyList
import org.http4s.blaze.client.BlazeClientBuilder

import scala.concurrent.ExecutionContext.global
import cats.effect._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.Declarations.NodeContextV6
import mx.cinvestav.commons.types.ObjectMetadata
import org.http4s.circe.CirceEntityCodec.{circeEntityDecoder, circeEntityEncoder}
import org.http4s.{Header, Headers, MediaType, Method, Request, Status, Uri}
import mx.cinvestav.Implicits._
import mx.cinvestav.commons.payloads.AddCacheNode
import org.typelevel.ci.CIString

case class ChordGetResponse(
                             key:String,
                             value:ObjectMetadata,
                             url:String,
                             milliSeconds:Long
                           )
case class ChordPut(key:String,value:String)
case class ChordNode(nodeId:String,host:String,port:Int){
  def url = s"http://$host:$port"
  def put(key:String,value:String): IO[Status] = {
    val body = ChordPut(key,value)
//      Json.obj("key"->key.asJson,"value"->value.asJson)
    val req = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(url+"/put"),
      headers = Headers(org.http4s.headers.`Content-Type`(MediaType.application.json)),
    )
      .withEntity(body)
    BlazeClientBuilder[IO](global).resource.use{ client=>
      client.status(req)
    }
  }
  def get(key:String) = {
    val req   = Request[IO](
      method  = Method.GET,
      uri     = Uri.unsafeFromString(url+s"/get/$key"),
    )
    BlazeClientBuilder[IO](global).resource.use{ client =>
      client.expect[ChordGetResponse](req)
    }
  }
}
case class KeyStoreInfo(
                         nodes:List[ChordNode]
                       )


case class LoadBalancerInfo(
                                exchange:String,
                                routingKey:String,
                                ip:String,
                                port:Int
                           ){
  def httpURL = s"http://$ip:$port"
  def addNodeUri = s"http://$ip:$port/api/v6/add-node"
  def addNode(payload:AddCacheNode)(implicit ctx:NodeContextV6) = for {
    timestamp          <- IO.realTime.map(_.toMillis)
    _                  <- ctx.logger.debug("ADD_NODE")
    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    req                = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(this.addNodeUri)
    ).withEntity(payload.asJson)
      .putHeaders(
        Headers(Header.Raw(CIString("Timestamp"),timestamp.toString))
      )
    status             <- client.status(req)
    _                  <- ctx.logger.debug(s"STATUS $status")
    _                  <- finalizer
  }  yield ()
}
case class LoadBalancerLevel(zero: LoadBalancerInfo, one:LoadBalancerInfo,cloud:LoadBalancerInfo)

case class Pool(hostname:String,port:Int) {
  def httpURL = s"http://$hostname:$port"
  def addNodeUri = s"http://$hostname:$port/api/v6/add-node"
  def addNode(payload:AddCacheNode)(implicit ctx:NodeContextV6) = for {
    timestamp          <- IO.realTime.map(_.toMillis)
//    _                  <- ctx.logger.debug("ADD_NODE")
    (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
    req                = Request[IO](
      method = Method.POST,
      uri = Uri.unsafeFromString(this.addNodeUri)
    ).withEntity(payload.asJson)
      .putHeaders(
        Headers(Header.Raw(CIString("Timestamp"),timestamp.toString))
      )
    status             <- client.status(req)
    _                  <- ctx.logger.debug(s"STATUS $status")
    _                  <- finalizer
  }  yield ()
}

case class DefaultConfigV5(
                            nodeId:String,
                            loadBalancer:LoadBalancerLevel,
                            replicationFactor:Int,
                            poolId:String,
                            storagePath:String,
                            cacheNodes:List[String],
                            cachePolicy:String,
                            cacheSize:Int,
                            port:Int,
                            host:String,
                            dropboxAccessToken:String,
                            //                            replicationStrategy:String,
                            totalStorageCapacity:Long,
                            totalMemoryCapacity:Long,
                            keyStore:KeyStoreInfo,
                            syncNodes:List[String],
                            clouds:List[String],
                            level:Int=0,
                            pool:Pool
                            //                          sourceFolders:List[String]
                        )

