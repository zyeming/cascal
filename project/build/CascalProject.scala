import sbt._
import de.element34.sbteclipsify._

class CascalProject(info:ProjectInfo) extends DefaultProject(info) with Eclipsify {
  val shorrockin = "Shorrockin Repository" at "http://maven.shorrockin.com"
}