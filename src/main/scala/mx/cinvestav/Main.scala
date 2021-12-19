package mx.cinvestav

import cats.data.NonEmptyList
import cats.effect.std.{Queue, Semaphore}
import cats.effect.{ExitCode, IO, IOApp}
import com.dropbox.core.DbxRequestConfig
import com.dropbox.core.v2.DbxClientV2
import mx.cinvestav.Declarations.{NodeContextV6, NodeStateV6}
import mx.cinvestav.monitoring.Monitoring
import org.http4s.blaze.client.BlazeClientBuilder
import retry._

import scala.concurrent.ExecutionContext.global
//org.http4s.client.middleware.RetryPolicy
//
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
//
import io.chrisdavenport.mapref.MapRef
import io.chrisdavenport.mules.MemoryCache.MemoryCacheItem
import io.chrisdavenport.mules._
//
import mx.cinvestav.Declarations.{ObjectS, RequestX}
import mx.cinvestav.commons.payloads.AddCacheNode
import mx.cinvestav.cache.CacheX.{LFU, LRU}
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
import concurrent.duration._


object Main extends IOApp{
  implicit val config: DefaultConfigV5 = ConfigSource.default.loadOrThrow[DefaultConfigV5]
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]
  val unsafeErroLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLoggerFromName[IO]("error")

  override def run(args: List[String]): IO[ExitCode] = {
        for {
          startTimestamp  <- IO.realTime.map(_.toMillis)
          _               <- Logger[IO].debug(s"CACHE NODE[${config.nodeId}] is up and running 🚀")
          //         __________________________________________________________________________
          mr                    <- MapRef.ofConcurrentHashMap[IO,String,MemoryCacheItem[ObjectS]](
            initialCapacity = 16,
            loadFactor = 0.75f,
            concurrencyLevel = 16
          )
          cache                 =  MemoryCache.ofMapRef[IO,String,ObjectS](
            mr = mr,
            defaultExpiration = None
          )
          currentEntries  <- IO.ref(List.empty[String])
          queue <- Queue.bounded[IO,RequestX](10)

           dbxConfig = DbxRequestConfig.newBuilder("cinvestav-cloud-test/1.0.0").build
           dbxClient = new DbxClientV2(dbxConfig, config.dropboxAccessToken)
           semaphore <- Semaphore[IO](1)
          dSemaphore <- Semaphore[IO](1)
          _initState      = NodeStateV6(
            levelId               = if(config.level==0) "LOCAL" else "SYNC",
            status                = status.Up,
            cacheNodes            = config.cacheNodes,
            ip                    = InetAddress.getLocalHost.getHostAddress,
            availableResources    = config.cacheNodes.length,
            cache                 =  cache,
            currentEntries        =  currentEntries,
            cacheSize             = config.cacheSize,
            queue                 = queue,
            cacheX                = if(config.cachePolicy=="LRU") LRU[IO,ObjectS](config.cacheSize) else LFU[IO,ObjectS](config.cacheSize),
            dropboxClient         =  dbxClient,
            s                     = semaphore,
            experimentId          =  config.experimentId
          )
          state           <- IO.ref(_initState)
          //        __________________________________________________________________________
          ctx             = NodeContextV6(config,logger = unsafeLogger,state=state,errorLogger = unsafeErroLogger)
          nodeMetadata = Map(
            "level" -> config.level.toString
          )
          addNodePayload = AddCacheNode(
            nodeId = config.nodeId,
            ip = _initState.ip,
            port = config.port,
            totalStorageCapacity = config.totalStorageCapacity,
            availableStorageCapacity = config.totalStorageCapacity,
            usedStorageCapacity = 0L,
            cacheSize =config.cacheSize,
            cachePolicy = config.cachePolicy,
            metadata = nodeMetadata,
            totalMemoryCapacity = config.totalMemoryCapacity,
            availableMemoryCapacity = config.totalMemoryCapacity,
            usedMemoryCapacity = 0L
          )
//         _ <- ctx.config.loadBalancer.zero.addNode(addNodePayload)(ctx=ctx)
          (client,finalizer) <- BlazeClientBuilder[IO](global).resource.allocated
          retryPolicy = RetryPolicies.exponentialBackoff[IO](1 seconds)
          connectToMid = ctx.config.pool.addNode(client)(addNodePayload)(ctx=ctx)
          connectToMidWithRetry <- retryingOnAllErrors[Unit](
            policy = retryPolicy,
            onError = (e:Throwable,details:RetryDetails)=>ctx.logger.error(e.getMessage) *> ctx.logger.debug(s"RETRY_DETAILS $details")
          )(connectToMid)
          _ <- Monitoring.run(client)(ctx).compile.drain.start
          _ <- HttpServer.run(dSemaphore)(ctx=ctx)
          _ <- finalizer
        } yield ExitCode.Success
      }
//    }
//  }.as(ExitCode.Success)
}
