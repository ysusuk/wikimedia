package com.iuriisusuk

import cats.Show
import cats.effect.{ Effect, IO }
import cats.implicits._
import cats.syntax.either._
import fs2.{ Sink, Stream, StreamApp }
import fs2.StreamApp.ExitCode
import io.circe.Json
import java.io.FileReader
import java.nio.file.Paths
import java.util.NoSuchElementException
import java.util.concurrent.Executors
import javax.xml.stream.events.Attribute
import javax.xml.stream.{ XMLEventReader, XMLInputFactory, XMLStreamConstants, XMLStreamReader }
import org.apache.commons.lang3.StringUtils
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor }
import scala.concurrent.ExecutionContext.Implicits.global


object Main extends StreamApp[IO] {

  override def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, ExitCode] = {

    case class See(name: String, lat: Option[String], long: Option[String], content: Option[String])
    implicit val seeShow: Show[See] = Show { see => s"${see.name},${see.lat.getOrElse("None")},${see.long.getOrElse("None")},${see.content.getOrElse("None")}" }
//    implicit val seeShorterShow: Show[See] = Show { see => s"${see.name},${see.lat.getOrElse("None")},${see.long.getOrElse("None")}" }
    case class Do(name: String, lat: Option[String], long: Option[String], content: Option[String])

    val blockingExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(2))

    val xmlInputFactory: XMLInputFactory = XMLInputFactory.newInstance
    xmlInputFactory.setProperty("javax.xml.stream.isCoalescing", true)

    // todo: filter.map can be replaced with collect - for this to be doable, we need dsl which represents events (e.g. case class StartElement, case class EndElement)
    // todo: {{do

//    fs2.text.ut
    Stream.force {
      IO(xmlInputFactory.createXMLEventReader(new FileReader(".//src//main//resources//enwikivoyage-20170620-pages-articles.xml"))).map { xmlEventReader =>
        Stream.repeatEval(IO(xmlEventReader.nextEvent))
          .filter(_.getEventType == XMLStreamConstants.START_ELEMENT)
          .map(_.asStartElement)
          .filter(_.getName.getLocalPart == "text")
          .map(_ => Option(xmlEventReader.peek))
          .unNone
          .filter(_.getEventType == XMLStreamConstants.CHARACTERS)
          .map(_.asCharacters.getData)
          .filter(_.contains("{{see"))
          .flatMap { str =>
            (Stream.emits(StringUtils.substringsBetween(str, "{{see", "}}")).map { str =>
              val m: Map[String, String] = str.split("\\|").filter(_.nonEmpty).map(_.trim).map { str =>
                val fields: Array[String] = str.trim().split('=')
                fields match {
                  // case Array(key) => (key, None)
                  case Array(key, value) => Some((key, value))
                  case _ => None
                }
              }.flatten.toMap
              m.get("name").map(name => See(name, m.get("lat"), m.get("long"), m.get("content")))
            }.unNone.covary[IO])
          }
      }}
      // .take(5)
      // .observe(Sink.showLinesStdOut[IO, See])
      .map(_.show)
      .intersperse("\n")
      .through(fs2.text.utf8Encode)
      .through(fs2.io.file.writeAll(Paths.get(".//pois.csv")))
      .handleErrorWith( th => {println(th); Stream.empty})
      .drain
      // handle/report errors properly
      .as(ExitCode.Success)
      .onFinalize(requestShutdown)
  }
}
