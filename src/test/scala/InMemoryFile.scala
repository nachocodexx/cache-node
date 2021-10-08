import cats.data.Chain
import cats.implicits._
import cats.effect._
import cats.effect.std.Queue
import fs2.Stream
import fs2.io.file.Files
import io.circe.Decoder.Result
import mx.cinvestav.Declarations.ObjectX
import mx.cinvestav.config.{ChordGetResponse, ChordNode}

import java.io.File
import java.nio.file.Paths
import concurrent.duration._
import language.postfixOps
import mx.cinvestav.Implicits._
import mx.cinvestav.cache.CacheX.{CacheResponse, LFU, LRU}
import mx.cinvestav.cache.cache.PutResponse

class InMemoryFile extends munit .CatsEffectSuite {
  final val TARGET   = "/home/nacho/Programming/Scala/cache-node/target"
  final val SOURCE_FOLDER  = s"$TARGET/source"
  final val SINK_FOLDER  = s"$TARGET/sink"
//  val ch0 = ChordNode("ch-0","69.0.0.5",5600)
  val ch0 = ChordNode("ch-0","localhost",5600)



  test("Chord requests"){
    val lru = LRU[IO,ObjectX](2)
    val o1 = ObjectX("UNO",Array.emptyByteArray,Map.empty[String,String])
    val o2 = ObjectX("DOS",Array.emptyByteArray,Map.empty[String,String])
    val o3 = ObjectX("TRES",Array.emptyByteArray,Map.empty[String,String])
    for {
      _    <- IO.println("INIT")
      printx = (x:CacheResponse) => IO.delay{println(x);println("______________________")}
      put0 <- lru.put("UNOO",o1)
      _    <- lru.get("UNOO")
      _    <- lru.get("UNOO")
      _    <- printx(put0)
      put1 <- lru.put("DOOS",o2)
      _    <- lru.get("DOOS")
      _    <- printx(put1)
      put2 <- lru.put("TRESS",o3)
      get2 <- lru.get("TRESS")
      _    <- printx(put2)
      _    <- printx(get2)
    } yield ()

  }

  test("Compress"){
    import java.io.FileInputStream
    import java.io.FileOutputStream
    import java.util.zip.ZipEntry
    import java.util.zip.ZipOutputStream
    val sourceFile = s"$SOURCE_FOLDER/0.pdf"
    val fos = new FileOutputStream(s"$SINK_FOLDER/compressed.zip")
    val zipOut = new ZipOutputStream(fos)
    val fileToZip = new File(sourceFile)
    val fis = new FileInputStream(fileToZip)
    val zipEntry = new ZipEntry(fileToZip.getName)
    zipOut.putNextEntry(zipEntry)
    val bytes = new Array[Byte](1024)
    var length = fis.read(bytes)
    println(length)
    while (length  >= 0) {
      zipOut.write(bytes, 0, length)
      length = fis.read(bytes)
    }
    zipOut.close()
    fis.close()
  }

  test("Queue"){
    val app = for {
      queue <- Queue.bounded[IO,Int](10)
      _ <- queue.offer(1)
      _ <- queue.offer(2)
      _ <- queue.offer(3)
      x <- queue.tryTake
      _ <- IO.println(s"ELEMENT $x")
      x <- queue.tryTake
      _ <- IO.println(s"ELEMENT $x")
      x <- queue.tryTake
      _ <- IO.println(s"ELEMENT $x")
      x <- queue.tryTake
      _ <- IO.println(s"ELEMENT $x")
    } yield ()
    app
  }
  test("Basics"){
    val path  = Paths.get(s"$TARGET/source/6.mkv")
    val bytesIO = Files[IO].readAll(path,8912).compile.to(Array)

    for {
      bytes <- bytesIO
      _     <- IO.delay(path.toFile.delete())
      _     <- IO.sleep(5 seconds)
      _     <- Stream.emits(bytes).through(Files[IO].writeAll(path)).compile.drain
    } yield ()

  }

}
