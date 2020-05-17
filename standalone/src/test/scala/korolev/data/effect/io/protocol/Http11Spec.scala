package korolev.data.effect.io.protocol

import korolev.data.ByteVector
import korolev.effect.Decoder
import org.scalatest.{FlatSpec, Matchers}

class Http11Spec extends FlatSpec with Matchers {

  import korolev.effect.io.protocol.Http11._

  "decodeLimitedBody" should "pass bytes if content length not reached and byteTotal is zero" in {
    val bytes = ByteVector.fill(5)(i => i.toByte)
    val contentLength = 10L
    val (bytesTotal, frame) = decodeLimitedBody(0, bytes, contentLength)
    bytesTotal shouldEqual 5
    frame shouldEqual Decoder.Action.Push(bytes)
  }

  it should "pass part of bytes and take back rest of bytes if content length reached" in {
    val bytes = ByteVector.fill(5)(i => i.toByte)
    val contentLength = 10L
    val (_, frame) = decodeLimitedBody(7, bytes, contentLength)
    frame shouldEqual Decoder.Action.ForkFinish(
      bytes.slice(0, 3),
      bytes.slice(3)
    )
  }

  it should "finish the stream if count of bytes in chunk is equals to content length" in {
    val bytes = ByteVector.fill(3)(i => i.toByte)
    val contentLength = 10L
    val (_, frame) = decodeLimitedBody(7, bytes, contentLength)
    frame shouldEqual Decoder.Action.PushFinish(bytes)
  }
}
