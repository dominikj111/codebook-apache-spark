object FlowImplicits {
	implicit class AnyExtensions[A](x: A) {
		def when(f: A => Boolean)(action: A => Unit): Unit = if (f(x)) action(x)
		def processAndGet(f: A => Unit): A = { f(x) ; x }
		def processAndGetResult(f: A => Any): Any = { f(x) }
		// def whenElse[B](f: A => Boolean)(action: A => B)(alterAction: A => B): B = if (f(x)) action(x) else alterAction(x)
	}
	implicit def ElvisOperator[A](right: A) { // NO TESTED
		def ?:(left: A) = if (left == null) right else left
	}
}
