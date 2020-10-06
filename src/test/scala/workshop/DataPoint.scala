package workshop

import java.util.Date

import scala.beans.BeanProperty

// java.util.Date is used here just to simplify examples (we don't need jsr310 support in json)
// in production use ZonedDateTime
case class DataPoint(@BeanProperty var time: Date, @BeanProperty var value: Double) {
  def this() = this(null, 0) // TODO should we allow nulls here?
}
