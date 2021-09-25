import cats.implicits._
import cats.effect._
import cats.effect.std.Queue
import fs2.Stream
import fs2.io.file.Files

import java.io.File
import java.nio.file.Paths
import concurrent.duration._
import language.postfixOps

class InMemoryFile extends munit .CatsEffectSuite {
  final val TARGET   = "/home/nacho/Programming/Scala/cache-node/target"
  final val SOURCE_FOLDER  = s"$TARGET/source"
  final val SINK_FOLDER  = s"$TARGET/sink"
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
    val path  = Paths.get(s"$TARGET/source/test.txt")
    val bytesIO = Files[IO].readAll(path,8912).compile.to(Array)

    for {
      bytes <- bytesIO
      _     <- IO.delay(path.toFile.delete())
      _     <- IO.sleep(5 seconds)
      _     <- Stream.emits(bytes).through(Files[IO].writeAll(path)).compile.drain
    } yield ()

  }

}
