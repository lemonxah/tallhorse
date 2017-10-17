package tallhorse.concurrent

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.control.NonFatal

/**
  * Project: test
  * Package: test.lemonxah
  * Created on 2/3/2017.
  * lemonxah aka lemonxah -
  * https://github.com/lemonxah
  * http://stackoverflow.com/users/2919672/lemon-xah
  */

/**
  * Task Type to represent lazy operations
  * Free monad like classes needed for trampolining
  */
case class Now[A](value: A) extends Task[A] {
  // optimization for skipping the run-loop on instant values
  override def runAsync(implicit ec: ExecutionContext): Future[A] = Future.successful(value)
}
case class Error[A](ex: Throwable) extends Task[A] {
  // optimization for skipping the run-loop on instant values
  override def runAsync(implicit ec: ExecutionContext): Future[A] = Future.failed(ex)
}
case class Eval[A](f: () ⇒ A) extends Task[A]
case class Suspend[A](trunk: () ⇒ Task[A]) extends Task[A]
case class Bind[A,B](source: Task[A], f: A ⇒ Task[B]) extends Task[B]

sealed class Task[+A] extends Serializable { self ⇒
  private type Current = Task[Any]
  private type Binder = Any ⇒ Task[Any]
  private type CallStack = mutable.ArrayStack[Binder]

  def createCallStack(): CallStack = mutable.ArrayStack()

  def runAsync(implicit ec: ExecutionContext): Future[A] = {
    val p: Promise[Any] = Promise()
    Future {
      startTrampolineLoop(self, null, null, p)
    }
    p.future.asInstanceOf[Future[A]]
  }

  private def startTrampolineLoop[A](source: Task[A], bindCurrent: Binder, bindRest: CallStack, p: Promise[Any]): Unit = {
    @tailrec def loop(source: Current, first: Binder, rest: CallStack): Unit = {
      source match {

        case Now(value) ⇒
          val bind =
            if(first ne null) first
            else if (rest ne null) { if(rest.nonEmpty) rest.pop() else null }
            else null
          if (bind eq null)
            p.success(value)
          else {
            val fa = try bind(value) catch { case NonFatal(ex) ⇒ Error(ex) }
            // Next
            loop(fa, null, rest)
          }

        case Eval(f) ⇒
          var errors = true
          var nextState: Current = null
          try {
            val bind =
              if (first ne null) first
              else if (rest ne null) { if(rest.nonEmpty) rest.pop() else null }
              else null
            val value = f()
            errors = false
            if (bind eq null) p.success(value)
            else try {
              nextState = bind(value)
            } catch { case NonFatal(ex) ⇒ p.failure(ex) }
          } catch {
            case NonFatal(ex) ⇒
              if (errors) p.failure(ex)
          }
          if (nextState ne null) {
            // Next
            loop(nextState, null, rest)
          }

        case Bind(fa, f) ⇒
          val bindNext = f.asInstanceOf[Binder]
          var callStack = rest

          if (first ne null) {
            if (callStack eq null) callStack = createCallStack()
            callStack.push(first)
          }
          // Next
          loop(fa, bindNext, callStack)

        case Suspend(trunk) ⇒
          val fa = try trunk() catch { case NonFatal(ex) ⇒ Error(ex) }
          // Next
          loop(fa, null, rest)

        case Error(ex) ⇒
          p.failure(ex)
      }
    }
    loop(source, bindCurrent, bindRest)
  }

}

/**
  * Companion object for the Task Class
  */
object Task {
  def apply[A](f: ⇒ A): Task[A] = Eval(() ⇒ f)
  def eval[A](f: ⇒ A): Task[A] = Eval(() ⇒ f)
  def suspend[A](f: ⇒ Task[A]): Task[A] = Suspend(() ⇒ f)
  def now[A](a: A): Task[A] = Now(a)
}

object TaskImplicits {
  implicit object TaskMonad extends Monad[Task] {
    override def bind[A, B](fa: Task[A])(f: (A) => Task[B]): Task[B] = Bind(fa, f)
    override def pure[A](a: A): Task[A] = Task.eval(a)
  }
}
