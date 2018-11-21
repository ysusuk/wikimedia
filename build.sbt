import Dependencies._

val http4sVersion = "0.18.1"
val circeVersion = "0.9.0"

lazy val root = (project in file("."))
  .settings(
    inThisBuild(List(
      organization := "com.iuriisusuk",
      scalaVersion := "2.12.7",
      version      := "0.1.0"
    )),
    name := "wikimedia-listings",
    mainClass in assembly := Some("com.iuriisusuk.Main"),
    libraryDependencies += scalaTest % Test,
    libraryDependencies ++= Seq(
      "org.http4s" %% "http4s-dsl" % http4sVersion,
      "org.http4s" %% "http4s-blaze-client" % http4sVersion,
      "org.http4s" %% "http4s-circe" % http4sVersion,
      "co.fs2" %% "fs2-core" % "0.10.3",
      "org.http4s" %% "jawn-fs2" % "0.10.3",
      "org.spire-math" %% "jawn-ast" % "0.11.0",
    ),
    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-core" % "1.0.1",
      "org.typelevel" %% "cats-effect" % "0.10"
    ),
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-optics" % circeVersion
    ),
    libraryDependencies += "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.292",
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.6.1",
    libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.8.1",
    scalacOptions += "-Ypartial-unification"
  )
  .enablePlugins(DockerPlugin)

trapExit := false

dockerfile in docker := {
  val artifact: File = assembly.value
  val artifactTargetPath = s"/app/${artifact.name}"

  new Dockerfile {
    from("openjdk:8-jre")
    add(artifact, artifactTargetPath)
    entryPoint("java", "-jar", "-Xms2048m", "-Xmx4096m", "-Xss1M", artifactTargetPath)
  }
}

imageNames in docker := Seq(ImageName(sys.env.get("IMAGE_NAME").getOrElse(s"local/${name.value}:latest")))
