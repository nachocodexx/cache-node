package mx.cinvestav.server.controllers

import cats.implicits._
import cats.effect._
import mx.cinvestav.Declarations.NodeContextV6
import org.http4s._
import org.http4s.dsl.io._
import org.http4s.HttpRoutes
import org.http4s.circe.CirceEntityEncoder._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._
import mx.cinvestav.commons.events.Put
import mx.cinvestav.events.Events
import mx.cinvestav.monitoring.Metrics
import mx.cinvestav.commons.balancer.v3.UF
import mx.cinvestav.commons.types.Monitoring.{MemoryInfo, NodeInfo}
//import mx.cinvestav.commons.types.Monitoring.Implicits._

object InfoRoutes {
  def apply()(implicit ctx:NodeContextV6) = {
    HttpRoutes.of[IO]{
      case req@GET -> Root / "info"=> for {
        currentState         <- ctx.state.get
        events               = Events.relativeInterpretEventsMonotonic(events=currentState.events)
        ramInfo              = MemoryInfo.toMB(Metrics.getRAMInfo)
        jvmRamInfo           = MemoryInfo.toMB(Metrics.getJVMMemoryInfo)
        systemCpu            = Metrics.getSystemCPU()
        cpu                  = Metrics.getCPU()
        ufRAM                = Metrics.getUfRAM()
        puts                 = Events.onlyPuts(events=events)
        totalStorageCapacity = ctx.config.totalStorageCapacity
        usedStorageCapacity  = puts.map(_.asInstanceOf[Put]).map(_.objectSize).sum
        availableStorageCapacity = totalStorageCapacity-usedStorageCapacity
        ufStorageCapacity = UF.calculate(total= totalStorageCapacity,used= usedStorageCapacity,objectSize=0)
        cacheSize            = ctx.config.cacheSize
        usedCacheSize        = puts.length
        availableCacheSize   = cacheSize - usedCacheSize
        ufCacheSize          = UF.calculate(total= cacheSize,used= usedCacheSize,objectSize=0)
        cachePolicy          = ctx.config.cachePolicy
        info                = NodeInfo(
          RAMInfo = ramInfo,
          JVMMemoryInfo = jvmRamInfo,
          systemCPUUsage = systemCpu,
          cpuUsage = cpu,
          RAMUf = ufRAM,
          cacheSize = cacheSize,
          usedCacheSize = usedCacheSize,
          availableCacheSize = availableCacheSize,
          ufCacheSize = ufCacheSize,
          cachePolicy = cachePolicy,
          totalStortageCapacity = totalStorageCapacity,
          usedStorageCapacity = usedStorageCapacity,
          availableStorageCapacity = availableStorageCapacity,
          ufStorageCapacity = ufStorageCapacity
        ).asJson
        _ <- ctx.logger.debug(s"CACHE_INFO")
        _ <- ctx.logger.debug(info.toString)
//        info                 = Json.obj(
//            "RAMInfo" -> ramInfo.asJson,
//          "JVMMemoryInfo" -> jvmRamInfo.asJson,
//          "systemCPUUsage" -> systemCpu.asJson,
//          "cpuUsage" -> cpu.asJson,
//          "RAMUf" -> ufRAM.asJson,
//          "cacheSize" -> cacheSize.asJson,
//          "usedCacheSize" -> usedCacheSize.asJson,
//          "availableCacheSize"-> ().asJson,
//          "ufCacheSize" ->.asJson,
//          "cachePolicy" -> cachePolicy.asJson,
//          "totalStorageCapacity" -> totalStorageCapacity.asJson,
//          "usedStorageCapacity" -> usedStorageCapacity.asJson,
//          "availableStorageCapacity" -> ().asJson,
//          "ufStorageCapacity" -> UF.calculate().asJson
////          "availableStorageCapacity" -> 0.asJson
//        )
        res <- Ok(info)
      } yield res
    }
  }
}
