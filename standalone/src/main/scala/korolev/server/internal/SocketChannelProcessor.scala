//package korolev.server.internal
//
//import java.nio.ByteBuffer
//import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}
//import java.util
//
//import korolev.Router.Path
//import korolev.effect.Effect
//import korolev.effect.io.LazyBytes
//import korolev.effect.Stream
//import korolev.effect.syntax._
//
//import korolev.server.{KorolevService, Request, Response}
//
//import scala.annotation.switch
//import scala.collection.mutable
//import scala.util.{Failure, Success}
//
//private[korolev] final class SocketChannelProcessor[F[_]: Effect](channel: AsynchronousSocketChannel,
//                                                                 service: KorolevService[F])
//    extends CompletionHandler[Integer, Unit] {
//
//  import SocketChannelProcessor._
//
//  private var state: Int = ReadHeader
//  private var buffer: ByteBuffer = ByteBuffer.allocate(MaxHeaderSize)
//  private var array = buffer.array()
//
//  private val writeBuffer = ByteBuffer.allocate(MaxHeaderSize)
//
//  channel.read(buffer, (), this)
//
//  def showNR(str: String) = {
//    str
//      .replaceAll("\n", "\\\\n\n")
//      .replaceAll("\r", "\\\\r")
//  }
//
//  private def buildString(start: Int, end: Int) = {
//    new String(array, start, end - start)
//  }
//
//  private def processBuffer(bytesRead: Int) = (state: @switch) match {
//    case ReadHeader =>
//      println(s"${buffer.toString} bytesRead = $bytesRead")
//      println(new String(buffer.array(), 0, buffer.position()))
//
//      val lastByteOfHeader = Http.locateLastHeader(array)
//      if (lastByteOfHeader > -1 && lastByteOfHeader <= buffer.position()) {
//        // Buffer contains header.
//        // Lets parse it.
//        val methodEnd = array.indexOf(' ')
//        val paramsStart = array.indexOf('?', methodEnd + 1)
//        val pathEnd = array.indexOf(' ', methodEnd + 1)
//        val protocolVersionEnd = array.indexOf('\r', pathEnd + 1)
//        // TODO method
//        val method = buildString(0, methodEnd)
//        val path = buildString(methodEnd + 1, if (paramsStart == -1) pathEnd else (paramsStart - 1))
//        val params = if (paramsStart == -1) null else buildString(paramsStart + 1, pathEnd)
//        val protocolVersion = buildString(pathEnd + 1, protocolVersionEnd)
//        // Parse headers.
//        val headers = mutable.Buffer.empty[(String, String)]
//        var headerStart = protocolVersionEnd + 2 // first line end plus \r\n chars
//        var maybeContentLength = Option.empty[Long]
//        var cookie: String = null
//        var webSocketKey: String = null
//        while (headerStart < lastByteOfHeader) {
//          val nameEnd = array.indexOf(':', headerStart)
//          val valueEnd = array.indexOf('\r', nameEnd)
//          Http.convertToLowerCase(array, headerStart, nameEnd)
//          val name = buildString(headerStart, nameEnd)
//          val value = buildString(nameEnd + 1, valueEnd).trim // TODO optimization available
//          if (name == "content-length") maybeContentLength = Some(value.toLong)
//          if (name == "sec-websocket-key") webSocketKey = value
//          if (name == "cookie") cookie = value
//          headers += ((name, value))
//          headerStart = valueEnd + 2
//        }
//        if (webSocketKey != null) {
//          // TODO websocket handshake
//        } else {
//          // Create body buffer
//          println(path)
//          val body = maybeContentLength match {
//            case Some(contentLength) =>
//              buffer.flip
//              buffer.position(lastByteOfHeader + 4)
//              val reader = new SocketChannelStream[F](buffer, contentLength, channel)
//              LazyBytes[F](reader, maybeContentLength)
//            case None =>
//              // TODO process unknown content length
//              LazyBytes.empty[F]
//          }
//          val request = Request[LazyBytes[F]](
//            path = Path.fromString(path),
//            param = Http.parseParams(params),
//            cookie = Http.parseCookie(cookie),
//            headers = headers,
//            body = body
//          )
//          println(request)
//          service.http(request) runAsync {
//            case Right(Response(status, body, headers)) =>
//              val finalHeaders = body.bytesLength.fold(headers) { size =>
//                headers :+ ("Content-Length", size.toString)
//              }
//              val responseHeader = Http.renderResponseHeader(status, finalHeaders)
//              LazyBytes(responseHeader) ++ body
//              body.chunks.foreach(SocketChannelProcessor.Write(channel))
//              new LazyBytesWriter(writeBuffer, body, channel, () => {
//                // Read next request
//                buffer.clear()
//                println(s"read next request ${writeBuffer}")
//                channel.read(buffer, (), this)
//              })
//            case Left(exception) =>
//              // TODO handle failure
//              exception.printStackTrace()
//          }
//        }
//      } else {
//        println(s"header is not completed")
//        println(showNR(new String(array)))
//        // Header is not completed
//        // Continue reading
//        channel.read(buffer, (), this)
//      }
//    case ReadBody =>
//      println("body")
//  }
//
//  // i.e. read completed
//  def completed(bytesRead: Integer, a: Unit): Unit = {
//    if (bytesRead == -1) println("socket closed")
//    else processBuffer(bytesRead)
//  }
//
//  def failed(throwable: Throwable, a: Unit): Unit =
//    throwable.printStackTrace()
//
//}
//
//final class SocketChannelStream[F[_]: Effect](buffer: ByteBuffer,
//                                              contentLength: Long,
//                                              channel: AsynchronousSocketChannel) extends Stream[F, Array[Byte]] {
//
//  private def showNR(str: String) = {
//    str
//      .replaceAll("\n", "\\\\n\n")
//      .replaceAll("\r", "\\\\r")
//  }
//
//  private var readTotal: Long = 0
//
//  def cancel(): F[Unit] = Effect[F].unit
//
//  def pull(): F[Option[Array[Byte]]] = Effect[F].promise { cb =>
//    if (readTotal == 0) {
//      val array = buffer.array().slice(buffer.position(), buffer.limit())
//      readTotal += array.length
//      println(s"first time: '${showNR(new String(array))}'")
//      cb(Right(Some(array)))
//    }
//    else if (readTotal < contentLength) {
//      buffer.clear()
//      channel.read(buffer, (), new CompletionHandler[Integer, Unit] {
//        def completed(bytesRead: Integer, notUsed: Unit): Unit = {
//          if (bytesRead < 0) {
//            // Socket was closed accidentally
//            cb(Right(None)) // TODO maybe it should be exception
//          } else {
//            readTotal += bytesRead
//            val array = buffer.array().slice(0, buffer.limit())
//            cb(Right(Some(array)))
//          }
//        }
//        def failed(throwable: Throwable, notUsed: Unit): Unit = {
//          cb(Left(throwable))
//        }
//      })
//    } else {
//      cb(Right(None))
//    }
//  }
//}
//
//private[korolev] object SocketChannelProcessor {
//
//  final val ReadHeader = 0
//  final val ReadBody = 1
//  final val MaxHeaderSize = 8192
//  final val RequestBodyBufferSize = 8192
//  final val ResponseBufferSize = 8192
//
//}
