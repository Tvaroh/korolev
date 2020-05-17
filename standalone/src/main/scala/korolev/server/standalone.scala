package korolev.server

import java.net.{InetSocketAddress, SocketAddress}
import java.nio.channels.AsynchronousChannelGroup
import java.util.concurrent.ExecutorService

import korolev.data.ByteVector
import korolev.effect.io.protocol.{Http11, WebSocketProtocol}
import korolev.effect.io.{LazyBytes, ServerSocket}
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
                  Http11.renderResponse(upgradedResponse2).foreach(client.write)
                }
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
