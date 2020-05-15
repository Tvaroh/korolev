import korolev.data.ByteVector
import org.scalatest.{FlatSpec, Matchers}

class ByteVectorSpec extends FlatSpec with Matchers {

  final val concatSample1 = ByteVector("abc".getBytes) :+ "def".getBytes
  final val concatSample2 = ("---".getBytes +: concatSample1 :+ "+++".getBytes)
  final val sliceSample1 = concatSample2.slice(3, 9)
  final val sliceSample2 = (sliceSample1 :+ "ghj".getBytes).slice(3, 9)
  final val sliceSample3 = sliceSample1.slice(0, 3)

  "asciiString" should "encode concatenated byte vector #1" in {
    concatSample1.asciiString shouldEqual "abcdef"
  }

  it should "encode concatenated byte vector #2" in {
    concatSample2.asciiString shouldEqual "---abcdef+++"
  }

  it  should "encode sliced byte vector #1" in {
    sliceSample1.asciiString shouldEqual "abcdef"
  }

  it should "encode sliced byte vector #2" in {
    sliceSample2.asciiString shouldEqual "defghj"
  }

  it should "encode sliced byte vector #3" in {
    println(sliceSample3)
    sliceSample3.asciiString shouldEqual "abc"
  }

  "length" should "be calculated correctly for concatenated byte vector" in {
    concatSample1.length shouldEqual 6
  }

  it should "be calculated correctly for sliced byte vector #1" in {
    sliceSample1.length shouldEqual 6
  }

  it should "be calculated correctly for sliced byte vector #2" in {
    sliceSample2.length shouldEqual 6
  }

  it should "be calculated correctly for sliced byte vector #3" in {
    sliceSample3.length shouldEqual 3
  }
}
