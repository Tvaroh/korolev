package korolev.server

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler, ServerSocketChannel}
import java.util.concurrent.ExecutorService

import korolev.Async
import korolev.server.internal.SocketChannelProcessor

object standalone {

  def buildServer[F[_]: Async](service: KorolevService[F],
                        host: String,
                        port: Int)
                       (implicit executorService: ExecutorService) = {

    val serverAddress = new InetSocketAddress(host, port)
    val asyncChannelGroup = AsynchronousChannelGroup.withThreadPool(executorService)
    val serverChannel = AsynchronousServerSocketChannel
      .open(asyncChannelGroup)
      .bind(serverAddress)

    object AcceptHandler extends CompletionHandler[AsynchronousSocketChannel, Unit] {
      def completed(clientChannel: AsynchronousSocketChannel, notUsed: Unit): Unit = {
        // Ready to accept new connection
        serverChannel.accept((), AcceptHandler)
        new SocketChannelProcessor(clientChannel, service)
      }
      def failed(throwable: Throwable, notUsed: Unit): Unit =
        throwable.printStackTrace()
    }
    serverChannel.accept((), AcceptHandler)
    Async[F].pure(serverChannel)
  }


}
