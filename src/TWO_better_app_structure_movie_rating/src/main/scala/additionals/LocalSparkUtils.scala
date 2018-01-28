
import scala.io.Codec
import scala.io.Source
import java.nio.charset.CodingErrorAction

object LocalSparkUtils {
	def loadDict[A, B](
		path: String,
		dataDelimiter: String,
		keyIndex: Int,
		valueIndex: Int,
		keyTransform: String => A,
		valueTransform: String => B ): Map[A, B] = {

			implicit val codec = Codec("UTF-8")
			codec.onMalformedInput(CodingErrorAction.REPLACE)
			codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

			var dict: Map[A, B] = Map()

			import FlowImplicits._, scala.math.max

			Source.fromFile(path).getLines().foreach(
				_.split(dataDelimiter)
					.when( _.length > max(keyIndex, valueIndex) )( x => dict += (keyTransform(x(keyIndex)) -> valueTransform(x(valueIndex))) )
			)

			return dict
	}
}