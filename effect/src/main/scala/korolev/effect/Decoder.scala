package korolev.effect

import korolev.effect.Decoder.Action
import korolev.effect.syntax._

class Decoder[F[_]: Effect, A](upstream: Stream[F, A]) extends Stream[F, A] { self =>

  @volatile private var takenBackQueue: List[A] = Nil

  def pull(): F[Option[A]] = Effect[F].delayAsync {
    takenBackQueue match {
      case Nil => upstream.pull()
      case x :: xs =>
        takenBackQueue = xs
        Effect[F].pure(Option(x))
    }
  }

  def cancel(): F[Unit] =
    upstream.cancel()

  def takeBack(item: A): F[Unit] =
    Effect[F].delay { takenBackQueue = item :: takenBackQueue }

  def decode[S, B](default: => S)(f: (S, A) => (S, Action[B])): Stream[F, B] =
    decodeAsync(default)((s, a) => Effect[F].pure(f(s, a)))

  def decodeAsync[S, B](default: => S)(f: (S, A) => F[(S, Action[B])]): Stream[F, B] =
    new Stream[F, B] {
      var state: S = default
      var finished = false
      def pull(): F[Option[B]] = Effect[F].delayAsync {
        if (finished) Effect[F].pure(None)
        else {
          self.pull().flatMap {
            case Some(x) =>
              f(state, x) flatMap {
                case (newState, Action.LastValue(value)) =>
                  state = newState
                  finished = true
                  Effect[F].pure(Some(value))
                case (newState, Action.Value(value)) =>
                  state = newState
                  Effect[F].pure(Some(value))
                case (newState, Action.Next) =>
                  state = newState
                  pull()
                case (newState, Action.End) =>
                  state = newState
                  Effect[F].pure(None)
              }
            case None => Effect[F].pure(None)
          }
        }
      }
      def cancel(): F[Unit] =
        self.cancel()
    }
}

object Decoder {

  sealed trait Action[+T]

  object Action {
    case class LastValue[+T](value: T) extends Action[T]
    case class Value[+T](value: T) extends Action[T]
    case object End extends Action[Nothing]
    case object Next extends Action[Nothing]
  }

  def apply[F[_]: Effect, A](upstream: Stream[F, A]): Decoder[F, A] =
    new Decoder(upstream)
}
