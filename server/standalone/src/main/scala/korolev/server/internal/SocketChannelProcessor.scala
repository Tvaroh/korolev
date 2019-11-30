package korolev.server.internal

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
import java.util

import korolev.{Async, LazyBytes}
import korolev.Router.Path
import korolev.server.{KorolevService, Request, Response}

import scala.annotation.switch
import scala.collection.mutable
import scala.util.{Failure, Success}

private[korolev] final class SocketChannelProcessor[F[_]: Async](channel: AsynchronousSocketChannel,
                                                                 service: KorolevService[F])
    extends CompletionHandler[Integer, Unit] {

  import SocketChannelProcessor._

  private var state: Int = ReadHeader
  private var buffer: ByteBuffer = ByteBuffer.allocate(MaxHeaderSize)
  private var array = buffer.array()

  private val writeBuffer = ByteBuffer.allocate(MaxHeaderSize)

  channel.read(buffer, (), this)

  def showNR(str: String) = {
    str
      .replaceAll("\n", "\\\\n\n")
      .replaceAll("\r", "\\\\r")
  }

  private def buildString(start: Int, end: Int) = {
    new String(array, start, end - start)
  }

  private def processBuffer(bytesRead: Int) = (state: @switch) match {
    case ReadHeader =>
      println(s"${buffer.toString} bytesRead = $bytesRead")
      println(new String(buffer.array(), 0, buffer.position()))

      val lastByteOfHeader = Http.locateLastHeader(array)
      if (lastByteOfHeader > -1 && lastByteOfHeader <= buffer.position()) {
        // Buffer contains header.
        // Lets parse it.
        val methodEnd = array.indexOf(' ')
        val paramsStart = array.indexOf('?', methodEnd + 1)
        val pathEnd = array.indexOf(' ', methodEnd + 1)
        val protocolVersionEnd = array.indexOf('\r', pathEnd + 1)
        // TODO method
        val method = buildString(0, methodEnd)
        val path = buildString(methodEnd + 1, if (paramsStart == -1) pathEnd else (paramsStart - 1))
        val params = if (paramsStart == -1) null else buildString(paramsStart + 1, pathEnd)
        val protocolVersion = buildString(pathEnd + 1, protocolVersionEnd)
        // Parse headers.
        val headers = mutable.Buffer.empty[(String, String)]
        var headerStart = protocolVersionEnd + 2 // first line end plus \r\n chars
        var maybeContentLength = Option.empty[Long]
        var cookie: String = null
        var webSocketKey: String = null
        while (headerStart < lastByteOfHeader) {
          val nameEnd = array.indexOf(':', headerStart)
          val valueEnd = array.indexOf('\r', nameEnd)
          Http.convertToLowerCase(array, headerStart, nameEnd)
          val name = buildString(headerStart, nameEnd)
          val value = buildString(nameEnd + 1, valueEnd).trim // TODO optimization available
          if (name == "content-length") maybeContentLength = Some(value.toLong)
          if (name == "sec-websocket-key") webSocketKey = value
          if (name == "cookie") cookie = value
          headers += ((name, value))
          headerStart = valueEnd + 2
        }
        if (webSocketKey != null) {
          // TODO websocket handshake
        } else {
          // Create body buffer
          println(path)
          val body = maybeContentLength match {
            case Some(contentLength) =>
              buffer.flip
              buffer.position(lastByteOfHeader + 4)
              val reader = new LazyBytesReader[F](buffer, contentLength, channel)
              LazyBytes[F](reader.pull, reader.finished, reader.cancel, maybeContentLength)
            case None =>
              LazyBytes.empty[F]
          }
          val request = Request[F](
            path = Path.fromString(path),
            param = Http.parseParams(params),
            cookie = Http.parseCookie(cookie),
            headers = headers,
            body = body
          )
          println(request)
          service.lift(request) match {
            case Some(eventuallyResponse) =>
              Async[F].runAsync(eventuallyResponse) {
                case Success(Response.Http(status, body, headers)) =>
                  writeBuffer.clear()
                  writeBuffer.put("HTTP/1.1 ".getBytes())
                  writeBuffer.put(status.codeAsString.getBytes())
                  writeBuffer.put(' '.toByte)
                  writeBuffer.put(status.phrase.getBytes())
                  def putNl() = {
                    writeBuffer.put('\r'.toByte)
                    writeBuffer.put('\n'.toByte)
                  }
                  def putHeader(name: String, value: String) = {
                    writeBuffer.put(name.getBytes)
                    writeBuffer.put(':'.toByte)
                    writeBuffer.put(' '.toByte)
                    writeBuffer.put(value.getBytes)
                    putNl()
                  }
                  putNl()
                  headers.foreach { header =>
                    val name = header._1
                    val value = header._2
                    putHeader(name, value)
                  }
                  body.size match {
                    case Some(size) => putHeader("Content-Length", size.toString)
                    case None => // do nothing
                  }
                  putNl()
                  writeBuffer.flip()
                  new LazyBytesWriter(writeBuffer, body, channel, () => {
                    // Read next request
                    buffer.clear()
                    println(s"read next request ${writeBuffer}")
                    channel.read(buffer, (), this)
                  })
                case Failure(exception) =>
                  // TODO handle failure
                  exception.printStackTrace()
              }
            case None =>
              // TODO handle no match, 404
              util.Arrays.fill(array, 0.toByte)
              channel.read(buffer, (), this)
          }
        }
      } else {
        println(s"header is not completed")
        println(showNR(new String(array)))
        // Header is not completed
        // Continue reading
        channel.read(buffer, (), this)
      }
    case ReadBody =>
      println("body")
  }

  // i.e. read completed
  def completed(bytesRead: Integer, a: Unit): Unit = {
    if (bytesRead == -1) println("socket closed")
    else processBuffer(bytesRead)
  }

  def failed(throwable: Throwable, a: Unit): Unit =
    throwable.printStackTrace()
}

final class LazyBytesReader[F[_]: Async](buffer: ByteBuffer,
                                         contentLength: Long,
                                         channel: AsynchronousSocketChannel)
  extends CompletionHandler[Integer, Unit] {

  def showNR(str: String) = {
    str
      .replaceAll("\n", "\\\\n\n")
      .replaceAll("\r", "\\\\r")
  }

  private var readTotal: Long = 0

  private var currentPromise: Async.Promise[F, Option[Array[Byte]]] = _

  private val finishedPromise = Async[F].promise[Unit]

  val finished: F[Unit] = finishedPromise.async

  val cancel = () => Async[F].unit

  val pull: () => F[Option[Array[Byte]]] = () => {
    val d = Async[F].delay {
      val firstTime = currentPromise == null
      currentPromise = Async[F].promise
      if (firstTime) {
        val array = buffer.array().slice(buffer.position(), buffer.limit())
        readTotal += array.length
        println(s"first time: '${showNR(new String(array))}'")
        currentPromise.complete(Success(Some(array)))
      }
      else if (readTotal < contentLength) {
        buffer.clear()
        channel.read(buffer, (), this)
      } else {
        finishedPromise.complete(Success(()))
        currentPromise.complete(Success(None))
      }
      currentPromise.async
    }
    Async[F].flatMap(d)(identity)
  }

  def completed(bytesRead: Integer, notUsed: Unit): Unit = {
    readTotal += bytesRead
    val array = buffer.array().slice(0, buffer.limit())
    currentPromise.complete(Success(Some(array)))
  }

  def failed(throwable: Throwable, notUsed: Unit): Unit = {
    if (currentPromise != null) {
      currentPromise.complete(Failure(throwable))
    } else {
      throwable.printStackTrace()
    }
  }
}

final class LazyBytesWriter[F[_]: Async](var buffer: ByteBuffer,
                                         lazyBytes: LazyBytes[F],
                                         channel: AsynchronousSocketChannel,
                                         onComplete: () => Unit)

  extends CompletionHandler[Integer, Unit] {

  write()

  private def write(): Unit = {
    //println(new String(buffer.array(), buffer.position(), buffer.remaining()))
    channel.write(buffer, (), this)
  }

  def completed(bytesWritten: Integer, a: Unit): Unit =
    if (buffer.hasRemaining()) {
      println(bytesWritten)
      write()
    }
    else {
      Async[F].runAsync(lazyBytes.pull()) {
        case Success(Some(bytes)) =>
          buffer = ByteBuffer.wrap(bytes)
          write()
        case Success(None) =>
          onComplete()
        case Failure(e) =>
          e.printStackTrace()
      }
    }

  def failed(throwable: Throwable, a: Unit): Unit =
    throwable.printStackTrace()
}

private[korolev] object SocketChannelProcessor {
  final val ReadHeader = 0
  final val ReadBody = 1
//  final val WebSocketHandsh
  final val MaxHeaderSize = 8192
  final val RequestBodyBufferSize = 8192
  final val ResponseBufferSize = 8192
}
