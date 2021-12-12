package mx.cinvestav.monitoring
import cats.implicits._
import cats.effect._
import fs2._
import mx.cinvestav.Declarations.NodeContextV6
import org.typelevel.ci.CIString
//
import org.http4s._
import org.http4s.Method._
import org.http4s.client.Client

import scala.concurrent.duration._
import language.postfixOps

object Monitoring {
  def run(client:Client[IO])(implicit ctx:NodeContextV6) = Stream.awakeEvery[IO](1 second).flatMap{ _=>
    for {
      _             <- Stream.eval(IO.unit)
      monitoringUrl = ctx.config.pool.monirotingUrl(ctx.config.nodeId)
      ramInfo       = Metrics.getRAMInfo.toMB
      jvmRamInfo = Metrics.getJVMMemoryInfo.toMB
      systemCpu     = Metrics.getSystemCPU()
      cpu           = Metrics.getCPU()
      ufRAM         = Metrics.getUfRAM()
//      jvmUfRAM      =

      req = Request[IO](
        method = POST,
        uri  = Uri.unsafeFromString(monitoringUrl),
        headers = Headers(
          Header.Raw(CIString("JVM-Total-RAM"),jvmRamInfo.total.toString),
          Header.Raw(CIString("JVM-Free-RAM"),jvmRamInfo.free.toString),
          Header.Raw(CIString("JVM-Used-RAM"),jvmRamInfo.used.toString),
          Header.Raw(CIString("JVM-UF-RAM"),Metrics.getUfRAMGen(jvmRamInfo).toString),
          Header.Raw(CIString("Total-RAM"),ramInfo.total.toString),
          Header.Raw(CIString("Free-RAM"),ramInfo.free.toString),
          Header.Raw(CIString("Used-RAM"),ramInfo.used.toString),
          Header.Raw(CIString("UF-RAM"),ufRAM.toString),
          Header.Raw(CIString("System-CPU-Load"),systemCpu.toString),
          Header.Raw(CIString("CPU-Load"),cpu.toString),
        )
      )
      response <- client.stream(req)
//      _ <- Stream.eval(ctx.logger.debug(s"MONITORING_STATUS ${response.status}"))
    } yield ()
  }

}
