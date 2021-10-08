package mx.cinvestav.config

import cats.data.NonEmptyList
import org.http4s.blaze.client.BlazeClientBuilder

import scala.concurrent.ExecutionContext.global
import cats.effect._
import io.circe.Json
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.types.ObjectMetadata
import org.http4s.circe.CirceEntityCodec.{circeEntityEncoder,circeEntityDecoder}
import org.http4s.{Header, Headers, MediaType, Method, Request, Status, Uri}
import mx.cinvestav.Implicits._

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
}
case class LoadBalancerLevel(zero: LoadBalancerInfo, one:LoadBalancerInfo,cloud:LoadBalancerInfo)

case class DefaultConfigV5(
                            nodeId:String,
                            loadBalancer:LoadBalancerLevel,
                            replicationFactor:Int,
                            poolId:String,
                            storagePath:String,
                            cacheNodes:List[String],
                            cachePolicy:String,
                            cacheSize:Int,
                            rabbitmq: RabbitMQClusterConfig,
                            port:Int,
                            host:String,
                            //                            replicationStrategy:String,
                            totalStorageSpace:Long,
                            keyStore:KeyStoreInfo,
                            syncNodes:List[String],
                            clouds:List[String],
                            level:Int=0
                            //                          sourceFolders:List[String]
                        )

