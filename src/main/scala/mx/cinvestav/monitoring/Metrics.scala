package mx.cinvestav.monitoring
import cats.implicits._
import cats.effect._
import com.sun.management.OperatingSystemMXBean

import java.lang.management.ManagementFactory
import mx.cinvestav.commons.balancer.v3.UF

object Metrics {
  object Implicits{
    implicit class BytesToMBConverter(bytes:Double) {
      def toMB = bytes/math.pow(1024,2)
    }
    implicit class BytesToMBConverterLong(bytes:Long) {
      def toMB = bytes/math.pow(1024,2)
    }
  }

  import Implicits._
  case class RAMInfo(total:Double,free:Double,used:Double) {
    def toMB = RAMInfo(total.toMB,free.toMB,used.toMB)
  }
  def getRAMInfo: RAMInfo = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val totalRAM = osMXBean.getTotalPhysicalMemorySize.toDouble
//    osMXBean.getFreeMe
    val freeRAM  = osMXBean.getFreePhysicalMemorySize.toDouble
    val usedRAM  = totalRAM-freeRAM
    RAMInfo(totalRAM,freeRAM,usedRAM)
  }
  def getJVMMemoryInfo: RAMInfo ={
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
    RAMInfo(total=maxMemory,free=totalFreeMemory,used= usedMemory)
  }
  def getUFJVMMemory(x:Double=0)={
    val ramInfo  = getJVMMemoryInfo
    UF.calculate(ramInfo.total,ramInfo.used,x)
  }
  def getUfRAM(x:Double=0.0):Double = {
    val ramInfo  = getRAMInfo
    UF.calculate(ramInfo.total,ramInfo.used,x)
  }
  def getUfRAMGen(ramInfo:RAMInfo,x:Double=0.0):Double = UF.calculate(ramInfo.total,ramInfo.used,x)


  def getCPU():Double = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    osMXBean.getProcessCpuLoad
  }
  def getSystemCPU():Double = {
    val osMXBean = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    osMXBean.getSystemCpuLoad
  }

}
