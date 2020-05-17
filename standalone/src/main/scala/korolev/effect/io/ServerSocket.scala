package korolev.effect.io

import java.net.SocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousChannelGroup, AsynchronousServerSocketChannel, AsynchronousSocketChannel, CompletionHandler}

import korolev.effect.{Effect, Stream}

/**
  * Stream API for AsynchronousServerSocketChannel.
  * Use `ServerSocket.bind` to start listening.
  * @see [[AsynchronousServerSocketChannel]]
  */
class ServerSocket[F[_]: Effect](channel: AsynchronousServerSocketChannel,
                                 readBufferSize: Int) extends Stream[F, Socket[F]] {

  def pull(): F[Option[Socket[F]]] = Effect[F].promise { cb =>
    channel.accept((), new CompletionHandler[AsynchronousSocketChannel, Unit] {
      def completed(socket: AsynchronousSocketChannel, notUsed: Unit): Unit = {
        cb(Right(Some(new Socket[F](socket, ByteBuffer.allocate(readBufferSize)))))
      }

      def failed(throwable: Throwable, notUsed: Unit): Unit =
        cb(Left(throwable))
    })
  }

  def cancel(): F[Unit] = Effect[F].delay(channel.close())
}

object ServerSocket {

  /**
    * Opens an AsynchronousServerSocketChannel and bind it to `socketAddress`.
    * @see [[AsynchronousServerSocketChannel]]
    */
  def bind[F[_]: Effect](socketAddress: SocketAddress,
                         backlog: Int = 0,
                         readBufferSize: Int = 8096,
                         group: AsynchronousChannelGroup = null): F[ServerSocket[F]] =
    Effect[F].delay {
      val channel = AsynchronousServerSocketChannel
        .open(group)
        .bind(socketAddress, backlog)
      new ServerSocket[F](channel, readBufferSize)
    }
}