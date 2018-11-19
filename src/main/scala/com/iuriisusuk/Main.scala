package com.iuriisusuk

import cats.Show
import cats.effect.{ Effect, IO }
import cats.implicits._
import cats.syntax.either._
import fs2.{ Sink, Stream, StreamApp }
import fs2.StreamApp.ExitCode
import io.circe.Json
import java.io.FileReader
import java.util.NoSuchElementException
import java.util.concurrent.Executors
import javax.xml.stream.events.Attribute
import javax.xml.stream.{ XMLEventReader, XMLInputFactory, XMLStreamConstants, XMLStreamReader }
import org.apache.commons.lang3.StringUtils
import scala.concurrent.{ ExecutionContext, ExecutionContextExecutor }
import scala.concurrent.ExecutionContext.Implicits.global


object Main extends StreamApp[IO] {

  override def stream(args: List[String], requestShutdown: IO[Unit]): Stream[IO, ExitCode] = {

    val xmlInputFactory: XMLInputFactory = XMLInputFactory.newInstance
    xmlInputFactory.setProperty("javax.xml.stream.isCoalescing", true)

    Stream.force {
      IO(xmlInputFactory.createXMLEventReader(new FileReader(".//src//main//resources//enwikivoyage-20170620-pages-articles.xml"))).map { xmlStreamReader =>
        Stream.repeatEval(IO(xmlStreamReader.nextEvent))
          .filter(_.getEventType == XMLStreamConstants.START_ELEMENT)
          .map(_.asStartElement)
          .filter(_.getName.getLocalPart == "text")
          .map(_ => Option(xmlStreamReader.peek))
          .unNone
          .filter(_.getEventType == XMLStreamConstants.CHARACTERS)
          .map(_.asCharacters.getData)
          .filter(_.contains("{{see"))
          .flatMap { str =>
            Stream.emits(StringUtils.substringsBetween(str, "{{see", "}}"))
          }
          .take(10)
      }
    }
      .observe(Sink.showLinesStdOut)
      // handle/report errors properly
      .handleErrorWith( _ => Stream.empty)
      .as(ExitCode.Success)
      .onFinalize(requestShutdown)
  }
}
