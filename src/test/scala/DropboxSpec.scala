import cats.effect.IO
import cats.implicits._
import fs2.io.file.Files
import mx.cinvestav.clouds.Dropbox

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, FileOutputStream, OutputStream}
//
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream
//
import com.dropbox.core.DbxRequestConfig
import com.dropbox.core.v2.DbxClientV2
import com.dropbox.core.v2.users.FullAccount
import java.io.FileInputStream
import java.io.InputStream
//
class DropboxSpec extends munit .CatsEffectSuite {
  final val ACCESS_TOKEN = "6n9TxLVwdIIAAAAAAAAAAYV7zgDdr3XQmf9QTgfdswVNM6RFGjH-Z9wDw9RQlMie"
  final val TARGET_PATH ="/home/nacho/Programming/Scala/cache-node/target"
  final val SOURCE_PATH = s"$TARGET_PATH/source"
  final val SINK_PATH = s"$TARGET_PATH/sink"

  test("Basics"){
    val config = DbxRequestConfig.newBuilder("cinvestav-cloud-test/1.0.0").build
    val client = new DbxClientV2(config, ACCESS_TOKEN)
    // Upload "test.txt" to Dropbox// Upload "test.txt" to Dropbox
//    val in = new FileInputStream(s"$SOURCE_PATH/0.pdf")
//
//    val  bytesIn = Files[IO].readAll(new File(s"$SOURCE_PATH/0.pdf").toPath,8192)
//    .compile
//    .to(Array)
//  .map(b=> new ByteArrayInputStream(b))
//    bytesIn.flatMap{ in=>
//      Dropbox.uploadObject(client)("0.pdf",in)
//    }

    //    DOWNLOAD
    val downloadFileStream:ByteArrayOutputStream = new ByteArrayOutputStream()
//    val bytes =
      Dropbox.downloadObject(dbxClientV2 = client)("0.pdf",downloadFileStream)
      .flatMap(x=>IO.println("BYTES_LEN "+x.length))
//    val metadata = client.files().downloadBuilder("/0.pdf").download(downloadFileStream)
//    downloadFileStream.close()
//    println(downloadFileStream.toByteArray.length)
  }

}
