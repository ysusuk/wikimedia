package com.iuriisusuk

import cats.Show
import cats.effect.IO
import cats.implicits._
import fs2.{ Sink, Stream, StreamApp }
import fs2.StreamApp.ExitCode
import java.io.FileReader
import java.nio.file.Paths
import javax.xml.stream.{ XMLInputFactory, XMLStreamConstants }
import org.apache.commons.lang3.StringUtils


object Main extends StreamApp[IO] {

  override def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, ExitCode] = {

    case class POI(name: String, lat: Option[String], long: Option[String], content: Option[String])
    implicit val poiShow: Show[POI] = Show.show { (poi: POI) => s"""${poi.name},${poi.lat.getOrElse("None")},${poi.long.getOrElse("None")},"${poi.content.getOrElse("None")}""""  }

//    case class See(name: String, lat: Option[String], long: Option[String], content: Option[String]) extends POI(name, lat, content)
//    case class Do(name: String, lat: Option[String], long: Option[String], content: Option[String]) extends POI(name, lat, content)

    val xmlInputFactory: XMLInputFactory = XMLInputFactory.newInstance
    xmlInputFactory.setProperty("javax.xml.stream.isCoalescing", true)

    // todo: filter.map can be replaced with collect - for this to be doable, we need dsl which represents events (e.g. case class StartElement, case class EndElement)
    // todo: path through random id and title
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
          .filter(str => str.contains("{{do") || str.contains("{{see"))
          .flatMap { str =>
            def transform(str: String, categoryTag: String): Stream[IO, POI] = {
              // StringUtils.substringsBetween returns null if no match found
              val pois: Array[String] = Option(StringUtils.substringsBetween(str, categoryTag, "}}")).getOrElse[Array[String]](Array.empty)
              Stream.emits(pois).map { str =>
                val m: Map[String, String] = str.split("\\|").filter(_.nonEmpty).map(_.trim).map { str =>
                  val fields: Array[String] = str.trim().split('=')
                  fields match {
                    // case Array(key) => (key, None)
                    case Array(key, value) => Some((key, value))
                    case _ => None
                  }
                }.flatten.toMap
                m.get("name").map(name => POI(name, m.get("lat"), m.get("long"), m.get("content")))
              }.unNone.covary[IO]
            }

           transform(str, "{{see").append(transform(str, "{{do"))
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
