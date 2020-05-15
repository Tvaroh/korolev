package korolev.server

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}
import java.util.concurrent.ExecutorService

import korolev.effect.{Effect, Reporter}
import korolev.effect.io.Http
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
        Http
          .processChannel(clientChannel, ByteBuffer.allocate(512), service)
          .foreach(_ => Effect[F].unit)
          .runAsyncForget(Reporter.PrintReporter)
      }
      def failed(throwable: Throwable, notUsed: Unit): Unit =
        throwable.printStackTrace()
    }
    serverChannel.accept((), AcceptHandler)
    serverChannel
  }


}
