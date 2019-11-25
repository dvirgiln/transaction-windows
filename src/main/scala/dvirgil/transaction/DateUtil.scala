package dvirgil.transaction

import java.util.{ Calendar, Date }
/*
 * Utility class for Date.
 */
object DateUtil {
  def getTruncSecond(date: Date): Long = {
    val c = Calendar.getInstance()
    c.setTime(date)
    c.set(Calendar.SECOND, 0)
    c.getTimeInMillis
  }
}
