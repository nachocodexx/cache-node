import sbt._


object Dependencies {
  def apply(): Seq[ModuleID] = {
//    lazy val RabbitMQUtils = "mx.cinvestav" %% "rabbitmq-utils" % "0.3.3"
    lazy val Commons = "mx.cinvestav" %% "commons" % "0.0.5"
    lazy val PureConfig = "com.github.pureconfig" %% "pureconfig" % "0.15.0"
    lazy val MUnitCats ="org.typelevel" %% "munit-cats-effect-3" % "1.0.3" % Test
    lazy val Log4Cats =   "org.typelevel" %% "log4cats-slf4j"   % "2.1.1"
    lazy val ScalaCompress = "com.github.gekomad" %% "scala-compress" % "1.0.0"
    lazy val Mules = "io.chrisdavenport" %% "mules"     % "0.5.0-M2"
    lazy val CatsNIO = "io.github.akiomik" %% "cats-nio-file" % "1.6.0"
    lazy val DropboxSDK = "com.dropbox.core" % "dropbox-core-sdk" % "4.0.1"
    lazy val CatsRetry = "com.github.cb372" %% "cats-retry" % "3.1.0"
    val http4sVersion = "1.0.0-M23"

    lazy val Logback   = "ch.qos.logback" % "logback-classic" % "1.2.3"
    val circeVersion = "0.14.1"
    lazy val Circe = Seq(
      "io.circe" %% "circe-core",
      "io.circe" %% "circe-generic",
      "io.circe" %% "circe-parser"
    ).map(_ % circeVersion)
    lazy val Http4s =Seq(
      "org.http4s" %% "http4s-dsl" ,
      "org.http4s" %% "http4s-blaze-server" ,
      "org.http4s" %% "http4s-blaze-client",
      "org.http4s" %% "http4s-circe"
    ).map(_ % http4sVersion)
    Seq(Logback,PureConfig,Commons,MUnitCats,Log4Cats,ScalaCompress,Mules,CatsNIO,DropboxSDK,CatsRetry)++Circe ++Http4s
  }
}


