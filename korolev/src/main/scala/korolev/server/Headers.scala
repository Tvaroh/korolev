/*
 * Copyright 2017-2020 Aleksey Fomkin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package korolev.server

object Headers {

  final val Connection = "connection"
  final val Upgrade = "upgrade"
  final val ContentType = "content-type"
  final val ContentLength = "content-length"
  final val SetCookie = "set-cookie"
  final val CacheControl = "cache-control"
  final val SecWebSocketKey = "sec-websocket-key"
  final val SecWebSocketAccept = "sec-websocket-accept"

  final val CacheControlNoCache = CacheControl -> "no-store, no-cache, must-revalidate"
  final val ContentTypeHtmlUtf8 = ContentType -> "text/html; charset=utf-8"
  final val ConnectionUpgrade = Connection -> "Upgrade"
  final val UpgradeWebSocket = Upgrade -> "websocket"

  def setCookie(cookie: String, value: String, path: String, maxAge: Int): (String, String) =
    SetCookie -> s"$cookie=$value; path=$path; max-age=$maxAge"
}
