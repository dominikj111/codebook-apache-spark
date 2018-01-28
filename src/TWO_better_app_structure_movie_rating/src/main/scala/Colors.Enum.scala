
import enumeratum._

sealed abstract class Colors(val brightness: Int) extends EnumEntry with Serializable {}

object Colors extends Enum[Colors] {

	case object WHITE extends Colors(3)
	case object GRAY  extends Colors(2)
	case object BLACK extends Colors(1)

	val values = findValues

	def darkest(a: Colors, b: Colors): Colors = if(a.brightness < b.brightness) a else b
	def brightest(a: Colors, b: Colors): Colors = if(a.brightness > b.brightness) a else b
}
