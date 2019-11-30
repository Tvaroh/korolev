package korolev.server

import korolev.Context

import scala.concurrent.Future
import korolev.execution._
import korolev.state.javaSerialization._

object TestApp extends App {

  import levsha.dsl._
  import html._
  val context = Context[Future, String, Any]
  import context._
  val config = KorolevServiceConfig[Future, String, Any](
    stateLoader = StateLoader.default("Hello world"),
    render = (state) => optimize {
      body(
        state,
        button(
          "Plus one",
          event("click")(_.transition(_ + "1"))
        )
      )
    }
  )
  val service = korolevService(mimeTypes, config)
//  val service: KorolevService[Future] = {
//    case Request(path, param, cookie, headers, body) =>
//      for {
//        num <- body.toStrictUtf8
//      } yield Response.Http(Response.Status.Ok, (num.toInt * 2).toString, Nil)
//  }

  standalone.buildServer[Future](service, "localhost", 8080)
  Thread.currentThread().join()
}
