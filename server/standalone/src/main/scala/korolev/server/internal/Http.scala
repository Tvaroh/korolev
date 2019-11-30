package korolev.server.internal

import java.net.URLDecoder

import korolev.Router.Path
import korolev.server.Request
import korolev.{Async, LazyBytes}
import java.util

import scala.annotation.tailrec
import java.nio.charset.StandardCharsets

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

  def parseParams(params: String): String => Option[String] = {
    lazy val map =
      if (params == null) Map.empty[String, String]
      else params
        .split('&')
        .map { xs =>
          val Array(k, v) = xs.split('=')
          (URLDecoder.decode(k, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8))
        }
        .toMap
    k => map.get(k)
  }

  def parseCookie(cookie: String): String => Option[String] = {
    lazy val map =
      if (cookie == null) Map.empty[String, String]
      else cookie
        .split(';')
        .map { xs =>
          val Array(k, v) = xs.split('=')
          (URLDecoder.decode(k.trim, StandardCharsets.UTF_8), URLDecoder.decode(v, StandardCharsets.UTF_8))
        }
        .toMap
    k => map.get(k)
  }
}
