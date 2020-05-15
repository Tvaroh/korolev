package korolev.server.internal

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import korolev.server.Response.Status

object Http {

  private final val HeaderDelimiter =
    Array[Byte]('\r', '\n', '\r', '\n')

  /**
    * Returns -1 if headers not finished yet
    * Returns n > 0 if where n is an index of byte next to
    * last byte of last header
    */
  def locateLastHeader(array: Array[Byte]): Int = {
    array.indexOfSlice(HeaderDelimiter)
  }

  def convertToLowerCase(array: Array[Byte], start: Int, end: Int): Unit = {
    def aux(i: Int): Unit = if (i <= end) {
      val c = array(i)
      if (c >= 'A' && c <= 'Z')
        array(i) = (c + 32).toByte
      aux(i + 1)
    }
    aux(start)
  }


}
