package korolev.data

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import scala.collection.GenSeq

sealed trait ByteVector {

  import ByteVector._

  def length: Long = this match {
    case Nope => 0L
    case Arr(array) => array.length
    case Concat(lhs, rhs) => lhs.length + rhs.length
    case Slice(bv, from, until) =>
      if (until > -1) until - from
      else bv.length - from
  }

  private def searchIndex(i: Long, reversed: Boolean = false)(f: (Long, Array[Byte]) => Long): Long =
    this match {
      case Nope => -1
      case Arr(array) => f(i, array)
      case Concat(lhs, rhs) if reversed =>
        val j = rhs.searchIndex(i)(f)
        if (j > -1) j else lhs.searchIndex(rhs.length)(f)
      case Concat(lhs, rhs)  =>
        val j = lhs.searchIndex(i)(f)
        if (j > -1) j else rhs.searchIndex(lhs.length)(f)
      case Slice(bv, from, until) =>
        val j = bv.searchIndex(i)(f)
        if (j >= from && j < until) j else -1
    }

  def indexOf(that: Byte): Long =
    indexOf(that, 0L)

  def indexOf(that: Byte, from: Long): Long =
    searchIndex(0) { (i, x) =>
      val j = x.indexOf(that, (from - i).toInt)
      if (j > -1) i + j
      else -1
    }

  def lastIndexOfSlice(that: GenSeq[Byte]): Long =
    searchIndex(0, reversed = true) { (i, x) =>
      val j = x.lastIndexOfSlice(that)
      if (j > -1) i + j
      else -1
    }

  def indexOfSlice(that: GenSeq[Byte]): Long =
    searchIndex(0) { (i, x) =>
      val j = x.indexOfSlice(that)
      if (j > -1) i + j
      else -1
    }


  def foreach(f: Byte => Unit): Unit = this match {
    case Nope => ()
    case Arr(array) => array.foreach(f)
    case Concat(lhs, rhs) =>
      lhs.foreach(f)
      rhs.foreach(f)
    case Slice(bv, from, until) =>
      var i = 0L
      bv.foreach { x =>
        if (i >= from && i < until) {
          f(x)
        }
        i = i + 1
      }
  }

  def mkArray: Array[Byte] = {
    val array = new Array[Byte](length.toInt)
    var i = 0
    foreach { x =>
      array(i) = x
      i = i + 1
    }
    array
  }

  def mkBuffer(buffer: ByteBuffer): Unit =
    foreach(buffer.put(_))

  def mkBuffer: ByteBuffer = {
    val buffer = ByteBuffer.allocate(length.toInt)
    mkBuffer(buffer)
    buffer
  }

  def slice(from: Long, until: Long): Slice =
    Slice(this, from, until)

  def utf8String: String =
    new String(mkBuffer.array(), StandardCharsets.UTF_8)

  def asciiString: String = {
    val builder = new StringBuilder
    foreach(byte => builder.append(byte.toChar))
    builder.mkString
  }

  def ++(bv: ByteVector): ByteVector =
    Concat(this, bv)

  def :+(array: Array[Byte]): ByteVector =
    Concat(this, Arr(array))

  def +:(array: Array[Byte]): ByteVector =
    Concat(Arr(array), this)
}

object ByteVector {

  case class Arr(array: Array[Byte]) extends ByteVector
  case class Concat(lhs: ByteVector, rhs: ByteVector) extends ByteVector
  case class Slice(bv: ByteVector, from: Long = 0, until: Long = -1) extends ByteVector
  case object Nope extends ByteVector {
    override def ++(bv: ByteVector): ByteVector = bv
    override def :+(array: Array[Byte]): ByteVector = ByteVector.Arr(array)
    override def +:(array: Array[Byte]): ByteVector = ByteVector.Arr(array)
  }

  def apply(array: Array[Byte]): ByteVector = ByteVector.Arr(array)
  val empty: ByteVector = Nope
}