package mx.cinvestav.monitoring
import cats.implicits._
import cats.effect._
import com.sun.management.OperatingSystemMXBean

import java.lang.management.ManagementFactory
import mx.cinvestav.commons.balancer.v3.UF
import mx.cinvestav.commons.types.Monitoring.MemoryInfo

object Metrics {

//  import Implicits._
//  case class RAMInfo(total:Double,free:Double,used:Double) {
//    def toMB = RAMInfo(total.toMB,free.toMB,used.toMB)
//  }
  def getRAMInfo: MemoryInfo = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val totalRAM = osMXBean.getTotalMemorySize.toDouble
    val freeRAM  = osMXBean.getFreeMemorySize.toDouble
    val usedRAM  = totalRAM-freeRAM
    MemoryInfo(totalRAM,freeRAM,usedRAM)
  }
  def getJVMMemoryInfo: MemoryInfo ={
    val runtime    = Runtime.getRuntime
    //    Total designated memory, this will equal the configured -Xmx value
    val maxMemory  = runtime.maxMemory()
    //    Total allocated memory, is the total allocated space reserved for the java process:
    val total      = runtime.totalMemory()
    //    Current allocated free memory, is the current allocated space ready for new objects
    val free       = runtime.freeMemory()
    //    Used memory, has to be calculated: totalMemory - freeMemory
    val usedMemory = total -free
    //  Total free memory, has to be calculated: maxMemory - usedMemory
    val totalFreeMemory = maxMemory - usedMemory
//    val used       = maxMemory - free
    MemoryInfo(total=maxMemory,free=totalFreeMemory,used= usedMemory)
  }
  def getUFJVMMemory(x:Double=0)={
    val ramInfo  = getJVMMemoryInfo
    UF.calculate(ramInfo.total,ramInfo.used,x)
  }
  def getUfRAM(x:Double=0.0):Double = {
    val ramInfo  = getRAMInfo
    UF.calculate(ramInfo.total,ramInfo.used,x)
  }
  def getUfRAMGen(ramInfo:MemoryInfo,x:Double=0.0):Double = UF.calculate(ramInfo.total,ramInfo.used,x)


  def getCPU():Double = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    osMXBean.getProcessCpuLoad
  }
  def getSystemCPU():Double = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    osMXBean.getSystemCpuLoad
  }

}
