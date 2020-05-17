package korolev.effect.io.protocol

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import korolev.Router.Path
import korolev.data.ByteVector
import korolev.effect.io.LazyBytes
import korolev.effect.{Decoder, Effect, Stream}
import korolev.server.Response.Status
import korolev.server.{Headers, Request, Response}

import scala.collection.mutable

object Http11 {

  private final val HeaderDelimiter =
    Array[Byte]('\r', '\n', '\r', '\n')

  private implicit final class StringBuilderOps(val builder: StringBuilder) extends AnyVal {
    def newLine(): StringBuilder = builder
      .append('\r')
      .append('\n')
  }

  def decodeRequest[F[_]: Effect](decoder: Decoder[F, ByteVector]): Stream[F, Request[LazyBytes[F]]] = decoder
      .decode(ByteVector.empty)(decodeHeader)
      .map { request =>
        request.copy(
          body = request.header(Headers.ContentLength).map(_.toLong) match {
            case Some(contentLength) =>
              val byteStream = decoder.decode(0L)(decodeLimitedBody(_, _, contentLength))
              LazyBytes(byteStream.map(_.mkArray), Some(contentLength)) // TODO bytevector
            case None => LazyBytes(decoder.map(_.mkArray), None)
          }
        )
      }

  def decodeHeader(buffer: ByteVector,
                   incoming: ByteVector): (ByteVector, Decoder.Action[ByteVector, Request[Unit]]) = {
    val allBytes = buffer ++ incoming
    allBytes.indexOfSlice(HeaderDelimiter) match {
      case -1 => (allBytes, Decoder.Action.TakeNext)
      case lastByteOfHeader =>
        val (bodyBytes, request) = parseRequest(allBytes, lastByteOfHeader)
        (ByteVector.empty, Decoder.Action.Fork(request, bodyBytes))
    }
  }

  def decodeLimitedBody[F[_] : Effect](prevBytesTotal: Long,
                                       incoming: ByteVector,
                                       contentLength: Long): (Long, Decoder.Action[ByteVector, ByteVector]) =
    prevBytesTotal + incoming.length match {
      case `contentLength` =>
        (contentLength, Decoder.Action.PushFinish(incoming))
      case total if total > contentLength =>
        val pos = contentLength - prevBytesTotal
        val value = incoming.slice(0, pos)
        val takeBack = incoming.slice(pos)
        // Push left slice to a downstream
        // and take back right slice to the upstream
        (total, Decoder.Action.ForkFinish(value, takeBack))
      case total =>
        (total, Decoder.Action.Push(incoming))
    }

  def parseRequest(allBytes: ByteVector,
                   lastByteOfHeader: Long): (ByteVector, Request[Unit]) = {
    // Buffer contains header.
    // Lets parse it.
    val methodEnd = allBytes.indexOf(' ')
    val paramsStart = allBytes.indexOf('?', methodEnd + 1)
    val pathEnd = allBytes.indexOf(' ', methodEnd + 1)
    val protocolVersionEnd = allBytes.indexOf('\r', pathEnd + 1)
    val method = allBytes.slice(0, methodEnd).asciiString
    val path = allBytes.slice(methodEnd + 1, if (paramsStart == -1) pathEnd else (paramsStart - 1)).asciiString
    val params = if (paramsStart == -1) null else allBytes.slice(paramsStart + 1, pathEnd).asciiString
    val protocolVersion = allBytes.slice(pathEnd + 1, protocolVersionEnd).asciiString
    // Parse headers.
    val headers = mutable.Buffer.empty[(String, String)]
    var headerStart = protocolVersionEnd + 2 // first line end plus \r\n chars
    var cookie: String = null
    while (headerStart < lastByteOfHeader) {
      val nameEnd = allBytes.indexOf(':', headerStart)
      val valueEnd = allBytes.indexOf('\r', nameEnd)
      val name = allBytes.slice(headerStart, nameEnd).asciiString.toLowerCase() // FIXME make header search case insensitive instead of this
      val value = allBytes.slice(nameEnd + 1, valueEnd).asciiString // TODO optimization available
      if (name == "cookie") cookie = value
      headers += ((name, value.trim))
      headerStart = valueEnd + 2
    }
    val request = Request(
      path = Path.fromString(path),
      param = parseParams(params),
      cookie = parseCookie(cookie),
      headers = headers,
      body = ()
    )
    val bodyBytes = allBytes.slice(lastByteOfHeader + 4, allBytes.length)
    (bodyBytes, request)
  }

  def renderResponseHeader(status: Status, headers: Seq[(String, String)]): String = {
    val builder = new StringBuilder()
    builder.append("HTTP/1.1 ")
      .append(status.codeAsString)
      .append(' ')
      .append(status.phrase)
      .newLine()
    def putHeader(name: String, value: String) = builder
      .append(name)
      .append(':')
      .append(' ')
      .append(value)
      .newLine()
    headers.foreach {
      case (name, value) =>
        putHeader(name, value)
    }
    builder
      .newLine()
      .mkString
  }

  def renderResponse[F[_]: Effect](response: Response[LazyBytes[F]]): Stream[F, Array[Byte]] = {
    val updatedHeaders = response.body.bytesLength match {
      case Some(s) => (Headers.ContentLength -> s.toString) +: response.headers
      case None => response.headers
    }
    val fullHeaderString = renderResponseHeader(response.status, updatedHeaders)
    val fullHeaderBytes = fullHeaderString.getBytes
    Stream.eval(fullHeaderBytes) ++ response.body.chunks
  }

  def parseParams(params: String): String => Option[String] = {
    lazy val map =
      if (params == null) Map.empty[String, String]
      else params
        .split('&')
        .map { xs =>
          val Array(k, v) = xs.split('=')
          (URLDecoder.decode(k, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8))
        }
        .toMap
    k => map.get(k)
  }

  def parseCookie(cookie: String): String => Option[String] = {
    lazy val map =
      if (cookie == null) Map.empty[String, String]
      else cookie
        .split(';')
        .map { xs =>
          val Array(k, v) = xs.split('=')
          (URLDecoder.decode(k.trim, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8))
        }
        .toMap
    k => map.get(k)
  }

}
