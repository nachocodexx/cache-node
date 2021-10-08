package mx.cinvestav

import cats.data.NonEmptyList
import cats.effect.std.Queue
import cats.effect.{ExitCode, IO, IOApp}
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
import mx.cinvestav.Declarations.{CommandIds, NodeContextV5, NodeStateV5, ObjectS, RequestX}
import mx.cinvestav.cache.CacheX.{LFU, LRU}
import mx.cinvestav.commons.balancer.v2.LoadBalancer
import mx.cinvestav.commons.commands.Identifiers
import mx.cinvestav.commons.payloads.v2.AddNode
import mx.cinvestav.commons.{balancer, status}
import mx.cinvestav.config.DefaultConfigV5
import mx.cinvestav.handlers.{Handlers, PrepareHandler, PromiseHandler, ProposeHandler, PullHandler, ReplicateHandler}
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
    RabbitMQUtils.initV2[IO](rabbitMQConfig){ implicit client=>
      client.createConnection.use{ implicit connection=>
        for {
          startTimestamp  <- IO.realTime.map(_.toMillis)
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
          lbExchangeName  = ExchangeName(config.loadBalancer.zero.exchange)
          lbRk            = RoutingKey(config.loadBalancer.zero.routingKey)
          lbExchangeName1  = ExchangeName(config.loadBalancer.one.exchange)
          lbRk1            = RoutingKey(config.loadBalancer.one.routingKey)
          loadBalancerCfg0 = PublisherConfig(exchangeName = lbExchangeName,routingKey = lbRk )
          loadBalancerCfg1 = PublisherConfig(exchangeName = lbExchangeName1,routingKey = lbRk1 )

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
          loadBalancerPublisher0 = PublisherV2(loadBalancerCfg0)
          loadBalancerPublisher1 = PublisherV2(loadBalancerCfg1)
          _initState      = NodeStateV5(
            levelId               = if(config.level==0) "LOCAL" else "SYNC",
            status                = status.Up,
            cacheNodes            = config.cacheNodes,
            ip                    = InetAddress.getLocalHost.getHostAddress,
            availableResources    = config.cacheNodes.length,
            availableStorageSpace = config.totalStorageSpace,
            usedStorageSpace      = 0L,
            cacheNodePubs         = cacheNodePubs,
            loadBalancerPublisherZero = loadBalancerPublisher0 ,
            loadBalancerPublisherOne = loadBalancerPublisher1 ,
            cache                 =  cache,
            currentEntries        =  currentEntries,
            cacheSize             = config.cacheSize,
            queue                 = queue,
            currentOperationId    = None,
            syncNodePubs          = syncNodePubs,
            syncLB =  LoadBalancer[String]("RB",NonEmptyList.fromListUnsafe(config.syncNodes)),
            cacheX = if(config.cachePolicy=="LRU") LRU[IO,ObjectS](config.cacheSize) else LFU[IO,ObjectS](config.cacheSize)
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
          //         PUBLISH TO LOADBALANCER

          nodeMetadata = Map(
            "cacheSize"->config.cacheSize.toString,
            "cachePolicy"->config.cachePolicy,
            "totalStorageSpace" -> config.totalStorageSpace.toString,
            "level" -> config.level.toString
          )
          addNodePayload = AddNode(
            nodeId = config.nodeId,
            ip     = _initState.ip,
            port   = config.port,
            timestamp = startTimestamp,
            metadata  = nodeMetadata

          ).asJson.noSpaces
          properties     = AmqpProperties(headers = Map("commandId"->  StringVal("ADD_NODE") ))
          lbMsg          = AmqpMessage[String](payload = addNodePayload, properties = properties)
          _              <- if(config.level==0 )
            loadBalancerPublisher0.publish(lbMsg).start
          else loadBalancerPublisher1.publish(lbMsg).start *> Logger[IO].debug("SENT TO LEVEL 1")
          //
          _ <- Handlers(queueName = queueName)(ctx=ctx).start
          _ <- HttpServer.run()(ctx=ctx)
        } yield ()
      }
    }
  }.as(ExitCode.Success)
}
