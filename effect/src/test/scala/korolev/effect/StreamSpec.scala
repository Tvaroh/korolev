package korolev.effect

import org.scalatest.{AsyncFlatSpec, FlatSpec, Matchers}

class StreamSpec extends AsyncFlatSpec with Matchers {

  "fold" should "accumulated all values left to right" in {
    Stream
      .eval(1,2,3).fold(0) { case (acc, x) => acc + x }
      .map(result => assert(result == 6))
  }

  "concat" should "concatenate two streams" in {
    (Stream.eval(1,2,3) ++ Stream.eval(4,5,6))
      .fold("") { case (acc, x) => acc + x }
      .map(result => assert(result == "123456"))
  }
}
