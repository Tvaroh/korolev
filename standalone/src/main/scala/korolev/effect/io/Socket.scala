package korolev.effect.io

import java.nio.ByteBuffer
import java.nio.channels.{AsynchronousSocketChannel, CompletionHandler}

import korolev.data.ByteVector
import korolev.effect.{Effect, Stream}

class Socket[F[_]: Effect](channel: AsynchronousSocketChannel,
                           buffer: ByteBuffer) extends Stream[F, ByteVector] {

  private var inProgress = false

  def pull(): F[Option[ByteVector]] = Effect[F].promise { cb =>
    buffer.clear()

    if (inProgress) println(s"${Console.RED}Concurrent pull() happened in Socket.red!${Console.RESET}")
    inProgress = true
    channel.read(buffer, (), new CompletionHandler[Integer, Unit] {
      def completed(bytesRead: Integer, notUsed: Unit): Unit = {
        inProgress = false
        if (bytesRead < 0) {
          // Socket was closed
          cb(Right(None))
        } else {
          val array = buffer.array().slice(0, bytesRead)
          cb(Right(Some(ByteVector(array))))
        }
      }

      def failed(throwable: Throwable, notUsed: Unit): Unit = {
        cb(Left(throwable))
      }
    })
  }

  def cancel(): F[Unit] =
    Effect[F].delay(channel.close())

  def write(bytes: ByteVector): F[Unit] = {
    val buffer = ByteBuffer.wrap(bytes.mkArray) // TODO Maybe it should be static allocated buffer
    Effect[F].promise { cb =>
      val handler = new CompletionHandler[Integer, Unit] {
        def completed(bytesWritten: Integer, notUsed: Unit): Unit =
          if (buffer.hasRemaining) channel.write(buffer, (), this)
          else cb(Right(()))
        def failed(throwable: Throwable, notUsed: Unit): Unit =
          cb(Left(throwable))
      }
      channel.write(buffer, (), handler)
    }
  }
}
