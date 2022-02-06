package mx.cinvestav.clouds
import cats.implicits._
import cats.effect._
import com.dropbox.core.v2.DbxClientV2
import com.dropbox.core.v2.files.FileMetadata
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.JavaConverters._

object Dropbox {

  def listObjects(client:DbxClientV2) = {
    IO.delay{
      val x = client.files().listFolder("")
      val y = x.getEntries.asScala.toList.map(_.getName)
      y
    }
  }
  def fileExists(client:DbxClientV2)(filename:String): IO[Boolean] =
    listObjects(client).map(_.contains(filename))
  //  <* IO.delay{in.close()}

  def uploadObject(dbxClient:DbxClientV2)(filename:String,in:ByteArrayInputStream): IO[FileMetadata] =
    IO.delay{
      dbxClient.files.uploadBuilder(s"/$filename").uploadAndFinish(in)
    } <* IO.delay{in.close()}

  def downloadObject(dbxClientV2: DbxClientV2)(filename:String,out:ByteArrayOutputStream): IO[Array[Byte]] =
    IO.delay{
//    dbxClientV2.files().downloadBuilder("/"+filename).download(out)
      dbxClientV2.files().downloadBuilder(s"/$filename").download(out)
    } *> IO.delay{
      out.toByteArray
    }

}
