package workshop

import java.util.Date

// java.util.Date is used here just to simplify examples (we don't need jsr310 support in json)
// in production use ZonedDateTime
case class DataPoint(time: Date, value: Double)
