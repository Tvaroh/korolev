package korolev.effect.io.protocol

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Base64

import korolev.data.ByteVector
import korolev.effect.{Decoder, Effect, Stream}
import korolev.effect.io.LazyBytes
import korolev.effect.syntax._

import korolev.server.Request.RequestHeader
import korolev.server.Response.Status
import korolev.server.{Headers, Request, Response}

import scala.annotation.{switch, tailrec}

/**
  * @see https://tools.ietf.org/html/rfc6455
  */
object WebSocketProtocol {

  final val OpCodeBinary = 0
  final val OpCodeText = 1
  final val OpCodePing = 9
  final val OpCodePong = 10
  final val GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11" // See RFC 4.2.2

  final case class Intention(key: String)

  sealed abstract class Frame(val opcode: Int)

  object Frame {
    case class Binary(payload: ByteVector) extends Frame(OpCodeBinary)
    case class Text(payload: String) extends Frame(OpCodeText)
    case object Ping extends Frame(OpCodePing)
    case object Pong extends Frame(OpCodePong)
  }

  sealed trait DecodingState

  object DecodingState {
    val begin: DecodingState = Begin
    case object Begin extends DecodingState
    case class ShortLength(fin: Boolean, mask: Boolean, opcode: Int) extends DecodingState
    case class LongLength(fin: Boolean, mask: Boolean, opcode: Int) extends DecodingState
    case class Payload(fin: Boolean, containMask: Boolean, opcode: Int, length: Long) extends DecodingState {
      lazy val offset: Long =
        if (containMask) 4
        else 0
      lazy val fullLength: Long =
        if (containMask) length + 4
        else length
    }
  }

  import DecodingState._

  def decodeFrame(data: ByteVector): ((ByteVector, DecodingState), Decoder.Action[ByteVector, Frame]) =
    decodeFrames(ByteVector.empty, Begin, data)

  @tailrec
  def decodeFrames(buffer: ByteVector,
                   state: DecodingState,
                   incoming: ByteVector): ((ByteVector, DecodingState), Decoder.Action[ByteVector, Frame]) = {

    def decodePayload(bytes: ByteVector, state: Payload): ByteVector =
      if (!state.containMask) {
        bytes.slice(0, state.fullLength)
      } else {
        val mask = bytes.slice(0, 4).mkArray
        bytes.slice(4, state.fullLength).mapI((x, i) => (x ^ mask(i % 4)).toByte)
      }

    val bytes = buffer ++ incoming
    state match {
      case Begin if bytes.length >= 2 =>
        // Enough to read length
        val firstByte = bytes(0) & 0xFF
        val secondByte = bytes(1) & 0xFF
        val fin = (firstByte & 0x80) != 0
        val opcode = firstByte & 0x0F
        val mask = (secondByte & 0x80) != 0
        val restOfBytes = bytes.slice(2)
        // Detect length encoding
        (secondByte & 0x7F: @switch) match {
          case 126 =>
            // Read 2 bytes length
            val nextState = ShortLength(fin, mask, opcode)
            decodeFrames(restOfBytes, nextState, ByteVector.empty)
          case 127 =>
            // Read 8 bytes length
            val nextState = LongLength(fin, mask, opcode)
            decodeFrames(restOfBytes, nextState, ByteVector.empty)
          case length =>
            // Tiny length. Read payload
            val nextState = Payload(fin, mask, opcode, length)
            decodeFrames(restOfBytes, nextState, ByteVector.empty)
        }
      case s: ShortLength if bytes.length >= 2 =>
        val length = bytes.readShort(0) & 0xFFFF
        val nextState = Payload(s.fin, s.mask, s.opcode, length)
        decodeFrames(bytes.slice(2), nextState, ByteVector.empty)
      case s: LongLength if bytes.length >= 8 =>
        val length = bytes.readLong(0)
        val nextState = Payload(s.fin, s.mask, s.opcode, length)
        decodeFrames(bytes.slice(8), nextState, ByteVector.empty)
      case s: Payload if bytes.length >= s.fullLength =>
        val payloadBytes = decodePayload(bytes, s)
        val frame = (s.opcode: @switch) match {
          case OpCodeBinary => Frame.Binary(payloadBytes)
          case OpCodeText => Frame.Text(payloadBytes.utf8String)
          case OpCodePing => Frame.Ping
          case OpCodePong => Frame.Pong
          case _ => throw UnsupportedOpcodeException(s.opcode)
        }
        bytes.slice(s.fullLength) match {
          case ByteVector.Empty => ((ByteVector.empty, Begin), Decoder.Action.Push(frame))
          case restOfBytes => ((ByteVector.empty, Begin), Decoder.Action.Fork(frame, restOfBytes))
        }
      case state =>
        // Not enough bytes in buffer. Keep buffering
        ((bytes, state), Decoder.Action.TakeNext)
    }
  }

  def encodeFrame(frame: Frame, maybeMask: Option[Int]): ByteVector = frame match {
    case Frame.Ping => encodeFrame(fin = true, maybeMask, frame.opcode, ByteVector.empty)
    case Frame.Pong => encodeFrame(fin = true, maybeMask, frame.opcode, ByteVector.empty)
    case Frame.Binary(payload) => encodeFrame(fin = true, maybeMask, frame.opcode, payload)
    case Frame.Text(payload) =>
      val bytes = ByteVector(payload.getBytes(StandardCharsets.UTF_8))
      encodeFrame(fin = true, maybeMask, frame.opcode, bytes)
    case _ => throw UnsupportedOpcodeException(frame.opcode)
  }

  def encodeFrame(fin: Boolean, maybeMask: Option[Int], opcode: Int, payload: ByteVector): ByteVector = {
    def choseLength[T](l: Long => T, s: Long => T, t: Long => T): T = payload.length match {
      case x if x > 65535 => l(x)
      case x if x > 125 => s(x)
      case x => t(x)
    }
    val maskSize = maybeMask.fold(0)(_ => 4)
    val buffer = ByteBuffer.allocate(2 + choseLength(_ => 8, _ => 2, _ => 0) + maskSize)
    buffer.put(((if (fin) 1 << 7 else 0) | opcode).toByte)
    buffer.put(((if (maybeMask.nonEmpty) 1 << 7 else 0 ) | choseLength[Int](_ => 127, _ => 126, _.toInt)).toByte)
    choseLength(
      x => buffer.putLong(x),
      x => buffer.putShort((x & 0xFFFF).toShort),
      _ => buffer
    )
    maybeMask match {
      case None => buffer.array() +: payload
      case Some(mask) =>
        val array = buffer.array()
        val o = array.length - 4
        buffer.putInt(mask)
        array +: payload.mapI { (x, i) =>
          (x ^ array(o + (i % 4))).toByte
        }
    }
  }

  def findIntention(request: RequestHeader): Option[Intention] =
    request.header(Headers.SecWebSocketKey).map(Intention)

  def handshake[T](response: Response[T], intention: Intention): Response[T] = {
    val kg = s"${intention.key}$GUID"
    val sha1 = MessageDigest.getInstance("SHA-1") // TODO optimize me
    val hash = sha1.digest(kg.getBytes(StandardCharsets.US_ASCII))
    val accept = Base64.getEncoder.encodeToString(hash)
    response.copy(
      status = Status.SwitchingProtocols,
      headers =
        (Headers.SecWebSocketAccept -> accept) +:
        Headers.ConnectionUpgrade +:
        Headers.UpgradeWebSocket +:
          response.headers
    )
  }

  def upgrade[F[_]: Effect](intention: Intention)
                           (f: Request[Stream[F, Frame]] => F[Response[Stream[F, Frame]]]): Request[LazyBytes[F]] => F[Response[LazyBytes[F]]] = f
      .compose[Request[LazyBytes[F]]] { request =>
        val messages = Decoder(request.body.chunks.map(ByteVector(_)))
          .decode((ByteVector.empty, DecodingState.begin)) {
            case ((buffer, state), incoming) =>
              decodeFrames(buffer, state, incoming)
          }
        request.copy(body = messages)
      }
      .andThen[F[Response[LazyBytes[F]]]] { responseF =>
        responseF.map { response =>
          val upgradedBody = response.body.map(m => encodeFrame(m, None).mkArray)
          handshake(response, intention).copy(body = LazyBytes(upgradedBody, None))
        }
      }

    case class UnsupportedOpcodeException(opcode: Int) extends Exception(s"Unsupported opcode: $opcode")
}
