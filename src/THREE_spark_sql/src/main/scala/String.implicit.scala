object StringImplicits {
	implicit class Editing(x: String) {
		def removeAll(toDelete: String): String = x.replace(toDelete,"")
	}
}
