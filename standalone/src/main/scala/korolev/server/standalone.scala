package korolev.server

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.ExecutorService

import korolev.data.ByteVector
import korolev.effect.io.protocol.{Http11, WebSocketProtocol}
import korolev.effect.{Decoder, Effect, Reporter}
import korolev.effect.io.{LazyBytes, Socket}
import korolev.effect.syntax._

object standalone {

  def buildServer[F[_]: Effect](service: KorolevService[F],
                        host: String,
                        port: Int)
                       (implicit executorService: ExecutorService) = Effect[F].delay {

    val serverAddress = new InetSocketAddress(host, port)
    val asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executorService)
    val serverChannel = AsynchronousServerSocketChannel
      .open(asyncChannelGroup)
      .bind(serverAddress)

    object AcceptHandler extends CompletionHandler[AsynchronousSocketChannel, Unit] {
      def completed(clientChannel: AsynchronousSocketChannel, notUsed: Unit): Unit = {
        // Ready to accept new connection
        serverChannel.accept((), AcceptHandler)
        val decoder = Decoder(Socket.read(clientChannel, ByteBuffer.allocate(512)))
        Http11
          .decodeRequest(decoder)
          .foreach { request =>
            WebSocketProtocol.findIntention(request) match {
              case Some(intention) =>
                // TODO should be moved to upgradeResponse
                val messages = Decoder(request.body.chunks.map(ByteVector(_)))
                  .decode((ByteVector.empty, WebSocketProtocol.DecodingState.begin)) {
                    case ((buffer, state), incoming) =>
                      WebSocketProtocol.decodeFrames(buffer, state, incoming)
                  }
                  .collect {
                    case WebSocketProtocol.Frame.Text(message) =>
                      message
                  }
                service.ws(request.copy(body = messages)) flatMap { response =>
                  val upgradedResponse = WebSocketProtocol.upgradeResponse(response, intention)
                  val upgradedBody = response.body.map(m => WebSocketProtocol.encodeFrame(WebSocketProtocol.Frame.Text(m), None).mkArray)
                  val upgradedResponse2 = upgradedResponse.copy(body = LazyBytes(upgradedBody, None))
                  Http11.renderResponse(upgradedResponse2).foreach(Socket.write(clientChannel))
                }
              case _ =>
                // This is just HTTP query
                service.http(request).flatMap { response =>
                  Http11.renderResponse(response).foreach(Socket.write(clientChannel))
                }
            }
          }
          .runAsyncForget(Reporter.PrintReporter)
      }
      def failed(throwable: Throwable, notUsed: Unit): Unit =
        throwable.printStackTrace()
    }
    serverChannel.accept((), AcceptHandler)
    serverChannel
  }


}
