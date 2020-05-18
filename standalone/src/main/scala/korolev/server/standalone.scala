package korolev.server

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.ExecutorService

import korolev.data.ByteVector
import korolev.effect.io.protocol.{Http11, RawWebSocketProtocol}
import korolev.effect.io.{LazyBytes, ServerSocket}
import korolev.effect.Stream

import korolev.effect.syntax._
import korolev.effect.{Decoder, Effect}

import scala.concurrent.ExecutionContext

object standalone {

  def buildServer[F[_]: Effect](service: KorolevService[F],
                                address: SocketAddress,
                                group: AsynchronousChannelGroup = null)
                               (implicit ec: ExecutionContext): F[Unit] = {
    ServerSocket.bind(address, group = group).flatMap { server =>
      server.foreach { client =>
        val decoder = Decoder(client)
        Http11
          .decodeRequest(decoder)
          .foreach { request =>
            RawWebSocketProtocol.findIntention(request) match {
              case Some(intention) =>
                val f = RawWebSocketProtocol.upgrade[F](intention) { (request: Request[Stream[F, RawWebSocketProtocol.Frame]]) =>
                  val b2 = request.body.collect {
                    case RawWebSocketProtocol.Frame.Text(message, _) =>
                      message.utf8String
                  }
                  // TODO service.ws should work with websocket frame
                  service.ws(request.copy(body = b2)).map { x =>
                    x.copy(body = x.body.map(m => RawWebSocketProtocol.Frame.Text(ByteVector.utf8(m), fin = true)))
                  }
                }
                f(request).flatMap(x => Http11.renderResponse(x).foreach(client.write))
              case _ =>
                // This is just HTTP query
                service.http(request).flatMap { response =>
                  Http11.renderResponse(response).foreach(client.write)
                }
            }
          }
          .start
          .unit
      }
    }
  }


}
