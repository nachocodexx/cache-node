package mx.cinvestav

import cats.data.NonEmptyList
import cats.effect.std.Queue
import cats.effect.{ExitCode, IO, IOApp}
import com.dropbox.core.DbxRequestConfig
import com.dropbox.core.v2.DbxClientV2
import mx.cinvestav.Declarations.{NodeContextV6, NodeStateV6}
//
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.utils.v2.encoders._
//
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.AmqpFieldValue.StringVal
import dev.profunktor.fs2rabbit.model.{AmqpFieldValue, AmqpMessage, AmqpProperties, ExchangeName, QueueName, RoutingKey}
//
import io.chrisdavenport.mapref.MapRef
import io.chrisdavenport.mules.MemoryCache.MemoryCacheItem
import io.chrisdavenport.mules._
//
import mx.cinvestav.Declarations.{ObjectS, RequestX}
import mx.cinvestav.commons.payloads.AddCacheNode
import mx.cinvestav.cache.CacheX.{LFU, LRU}
import mx.cinvestav.commons.balancer.v2.LoadBalancer
import mx.cinvestav.commons.status
import mx.cinvestav.config.DefaultConfigV5
import mx.cinvestav.server.HttpServer
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.v2._
//
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
//
import pureconfig.ConfigSource
import pureconfig.generic.auto._
//
import java.io.{ByteArrayOutputStream, File}
import java.net.InetAddress
import scala.language.postfixOps

object Main extends IOApp{
  implicit val config: DefaultConfigV5 = ConfigSource.default.loadOrThrow[DefaultConfigV5]
  val rabbitMQConfig: Fs2RabbitConfig  = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]


  override def run(args: List[String]): IO[ExitCode] = {
        for {
          startTimestamp  <- IO.realTime.map(_.toMillis)
          _               <- Logger[IO].debug(config.toString)
          _               <- Logger[IO].debug(s"CACHE NODE[${config.nodeId}] is up and running 🚀")
          //         __________________________________________________________________________
          mr                    <- MapRef.ofConcurrentHashMap[IO,String,MemoryCacheItem[Int]](
            initialCapacity = 16,
            loadFactor = 0.75f,
            concurrencyLevel = 16
          )
          cache                 =  MemoryCache.ofMapRef[IO,String,Int](
            mr = mr,
            defaultExpiration = None
          )
          currentEntries  <- IO.ref(List.empty[String])
          queue <- Queue.bounded[IO,RequestX](10)

           dbxConfig = DbxRequestConfig.newBuilder("cinvestav-cloud-test/1.0.0").build
           dbxClient = new DbxClientV2(dbxConfig, config.dropboxAccessToken)
          _initState      = NodeStateV6(
            levelId               = if(config.level==0) "LOCAL" else "SYNC",
            status                = status.Up,
            cacheNodes            = config.cacheNodes,
            ip                    = InetAddress.getLocalHost.getHostAddress,
            availableResources    = config.cacheNodes.length,
//            availableStorageSpace = config.totalStorageSpace,
//            usedStorageSpace      = 0L,
            cache                 =  cache,
            currentEntries        =  currentEntries,
            cacheSize             = config.cacheSize,
            queue                 = queue,
//            currentOperationId    = None,
//            syncLB                =  LoadBalancer[String]("RB",NonEmptyList.fromListUnsafe(config.syncNodes)),
            cacheX                = if(config.cachePolicy=="LRU") LRU[IO,ObjectS](config.cacheSize) else LFU[IO,ObjectS](config.cacheSize),
            dropboxClient         =  dbxClient
          )
          state           <- IO.ref(_initState)
          //        __________________________________________________________________________
          ctx             = NodeContextV6(config,logger = unsafeLogger,state=state)
          nodeMetadata = Map(
            "level" -> config.level.toString
          )
          addNodePayload = AddCacheNode(
            nodeId = config.nodeId,
            ip = _initState.ip,
            port = config.port,
            totalStorageCapacity = config.totalStorageSpace,
            availableStorageCapacity = config.totalStorageSpace,
            usedStorageCapacity = 0L,
            cacheSize =config.cacheSize,
            cachePolicy = config.cachePolicy,
            metadata = nodeMetadata
          )
          _ <- ctx.config.level match {
            case 0 => ctx.config.loadBalancer.zero.addNode(addNodePayload)(ctx=ctx)
            case 1 => ctx.config.loadBalancer.one.addNode(addNodePayload)(ctx=ctx)
            case _ => IO.println("NOPE")
          }

//          addNodePayload = AddNode(
//            nodeId = config.nodeId,
//            ip     = _initState.ip,
//            port   = config.port,
//            timestamp = startTimestamp,
//            metadata  = nodeMetadata
//
//          ).asJson.noSpaces
          _ <- HttpServer.run()(ctx=ctx)
        } yield ExitCode.Success
      }
//    }
//  }.as(ExitCode.Success)
}
