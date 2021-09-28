package mx.cinvestav

import cats.data.NonEmptyList
import cats.effect.std.Queue
import cats.effect.{ExitCode, IO, IOApp}
import dev.profunktor.fs2rabbit.config.Fs2RabbitConfig
import dev.profunktor.fs2rabbit.model.{AmqpFieldValue, ExchangeName, QueueName, RoutingKey}
import io.chrisdavenport.mapref.MapRef
import io.chrisdavenport.mules.MemoryCache.MemoryCacheItem
import io.chrisdavenport.mules._
import mx.cinvestav.Declarations.{CommandIds, NodeContextV5, NodeStateV5, RequestX}
import mx.cinvestav.commons.balancer.v2.LoadBalancer
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.commons.{balancer, status}
import mx.cinvestav.config.DefaultConfigV5
import mx.cinvestav.handlers.{Handlers, PrepareHandler, PromiseHandler, ProposeHandler, PullHandler, ReplicateHandler}
import mx.cinvestav.server.HttpServer
import mx.cinvestav.utils.RabbitMQUtils
import mx.cinvestav.utils.v2._
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.log4cats.{Logger, SelfAwareStructuredLogger}
import pureconfig.ConfigSource
import pureconfig.generic.auto._

import java.io.{ByteArrayOutputStream, File}
import java.net.InetAddress
import scala.language.postfixOps

object Main extends IOApp{
  implicit val config: DefaultConfigV5 = ConfigSource.default.loadOrThrow[DefaultConfigV5]
  val rabbitMQConfig: Fs2RabbitConfig  = RabbitMQUtils.parseRabbitMQClusterConfig(config.rabbitmq)
  implicit val unsafeLogger: SelfAwareStructuredLogger[IO] = Slf4jLogger.getLogger[IO]


  override def run(args: List[String]): IO[ExitCode] = {
    RabbitMQUtils.initV2[IO](rabbitMQConfig){ implicit client=>
      client.createConnection.use{ implicit connection=>
        for {
          _               <- Logger[IO].debug(config.toString)
          _               <- Logger[IO].debug(s"CACHE NODE[${config.nodeId}] is up and running 🚀")
          //        __________________________________________________________________________
          implicit0(rabbitMQContext:RabbitMQContext) <- IO.pure(RabbitMQContext(client = client,connection=connection))
          rootFile        = new File("/")
          exchangeName   = ExchangeName(config.poolId)
          //        __________________________________________________________________________
          cacheNodePubs = config.cacheNodes.map{ sn =>
              (sn,PublisherConfig(exchangeName =exchangeName,routingKey = RoutingKey(s"${config.poolId}.$sn")))
          }.map{ case (snId, config) => (snId,PublisherV2(config))}.toMap
          syncNodePubs = config.syncNodes.map{ sn =>
            (sn,PublisherConfig(exchangeName =exchangeName,routingKey = RoutingKey(s"${config.poolId}.$sn")))
          }.map{ case (snId, config) => (snId,PublisherV2.create(snId,config))}.toMap
          //
          lbExchangeName = ExchangeName(config.loadBalancer.exchange)
          lbRk           = RoutingKey(config.loadBalancer.routingKey)
          loadBalancerCfg = PublisherConfig(exchangeName = lbExchangeName,routingKey = lbRk )
          //         __________________________________________________________________________
          mr                    <- MapRef.ofConcurrentHashMap[IO,String,MemoryCacheItem[Int]](
            initialCapacity = 16,
            loadFactor = 0.75f,
            concurrencyLevel = 16
          )
          mrv2                  <- MapRef.ofConcurrentHashMap[IO,String,MemoryCacheItem[ByteArrayOutputStream]](
            initialCapacity = 16,
            loadFactor = 0.75f,
            concurrencyLevel = 16
          )
          cache                 =  MemoryCache.ofMapRef[IO,String,Int](
            mr = mr,
            defaultExpiration = None
          )
          cachev2               =  MemoryCache.ofMapRef[IO,String,ByteArrayOutputStream](
            mr = mrv2,
            defaultExpiration = None
          )
          currentEntries  <- IO.ref(List.empty[String])
          queue <- Queue.bounded[IO,RequestX](10)
          _initState      = NodeStateV5(
            status                = status.Up,
            loadBalancer          = balancer.LoadBalancer(config.loadBalancer.strategy),
            cacheNodes            = config.cacheNodes,
            ip                    = InetAddress.getLocalHost.getHostAddress,
            availableResources    = config.cacheNodes.length,
            replicationStrategy   = config.replicationStrategy,
            freeStorageSpace      = rootFile.getFreeSpace,
            usedStorageSpace      = rootFile.getTotalSpace - rootFile.getFreeSpace,
            cacheNodePubs         = cacheNodePubs,
            loadBalancerPublisher = PublisherV2(loadBalancerCfg),
//            keyStore              = PublisherV2(keyStoreCfg),
            cache                 =  cache,
            cachev2               = cachev2,
//              newCache,
            currentEntries        =  currentEntries,
            cacheSize             = config.cacheSize,
            queue                 = queue,
            currentOperationId    = None,
            syncNodePubs          = syncNodePubs,
            syncLB =  LoadBalancer[String]("RB",NonEmptyList.fromListUnsafe(config.syncNodes))
          )
          state           <- IO.ref(_initState)
          //        __________________________________________________________________________
          _ <- Exchange.topic(exchangeName = exchangeName)
          queueName = QueueName(s"${config.nodeId}")
          routingKey = RoutingKey(s"${config.poolId}.${config.nodeId}")
          _ <- MessageQueue.createThenBind(
            queueName = queueName,
            exchangeName=exchangeName,
            routingKey = routingKey
          )
          ctx             = NodeContextV5(config,logger = unsafeLogger,state=state,rabbitMQContext = rabbitMQContext)
          _ <- Handlers(queueName = queueName)(ctx=ctx).start
          _ <- HttpServer.run()(ctx=ctx)
        } yield ()
      }
    }
  }.as(ExitCode.Success)
}
