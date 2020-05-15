package korolev.server

import java.util.concurrent.Executors

import korolev.Context
import korolev.state.javaSerialization._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}

object TestApp extends App {

  import levsha.dsl._
  import html._
  val context = Context[Future, String, Any]
  import context._

  implicit val ec = ExecutionContext
    .fromExecutorService(Executors.newCachedThreadPool())

  val config = KorolevServiceConfig[Future, String, Any](
    stateLoader = StateLoader.default("Hello world"),
    head = _ => Seq(
      link(rel := "stylesheet", href := "static/main.css")
    ),
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
  val service = korolevService(config)
//  val service: KorolevService[Future] = {
//    case Request(path, param, cookie, headers, body) =>
//      for {
//        num <- body.toStrictUtf8
//      } yield Response.Http(Response.Status.Ok, (num.toInt * 2).toString, Nil)
//  }

  standalone.buildServer[Future](service, "localhost", 8080)
  Thread.currentThread().join()
}
