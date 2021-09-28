package mx.cinvestav

import io.circe.{Decoder, DecodingFailure, HCursor, parser}
import mx.cinvestav.commons.types.ObjectMetadata
import mx.cinvestav.config.ChordGetResponse
import cats.implicits._
import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object Implicits {

  implicit val chordGetResponseDecoder:Decoder[ChordGetResponse]= (c: HCursor) => for {
    key <- c.downField("key").as[String]
    url <- c.downField("url").as[String]
    milliSeconds <- c.downField("milliSeconds").as[Long]
    value <- c.downField("value").as[String]
      .flatMap(parser.parse)
      .flatMap(_.as[ObjectMetadata])
      .leftMap(t => DecodingFailure.fromThrowable(t.getCause, Nil))
  } yield ChordGetResponse(key, value, url, milliSeconds)

}
