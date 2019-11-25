package dvirgil.transaction

import java.text.SimpleDateFormat
import java.util.Date

import DateUtil._
import Domain._

/*
 * Contains the case classes used as Domain of the exercise.
 */
object Domain {
  val sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")

  // Case class that contains the logic of the account associated to the transactions. For the exercise has no relevance.
  case class Account(name: String, sortCode: String, accountNumber: String)

  case class Transaction(date: Date, from: Account, to: Account, amount: Float)

  case class WindowResult(startTs: Long, endTs: Long, result: Boolean) {
    def prettyPrint: String = s"${sdf.format(new Date(startTs))} - ${sdf.format(new Date(endTs))} = $result"
  }

  //Contains a type parameter with the type of result.
  case class WindowAggregation[A](startTs: Long, endTs: Long, result: A)
}

/*
 * Trait that contains the definition of how to apply operations to one window. The window operation
 */
trait WindowOperation[A] {
  /*
   * Defines how to add a transaction to a Window
   */
  def aggregateFunc: (WindowAggregation[A], Transaction) => WindowAggregation[A]

  /*
   * Defines how to generate a result from an aggregated window.
   */
  def resultFunc: (WindowAggregation[A]) => WindowResult

  /*
   * Defines the initial empty value.
   */
  def empty: A
}

class SumWindowOperation(limit: Float) extends WindowOperation[Float] {
  def aggregateFunc = (w, t) => w.copy(result = (w.result + t.amount))

  def resultFunc = (w) => WindowResult(w.startTs, w.endTs, w.result > limit)

  def empty: Float = 0.0F
}

class CountWindowOperation(limit: Float) extends WindowOperation[Int] {
  def aggregateFunc = (w, t) => w.copy(result = (w.result + 1))

  def resultFunc = (w) => WindowResult(w.startTs, w.endTs, w.result >= limit)

  def empty: Int = 0
}
/*
  This object contains the logic to analyze a list of transactions and generate a stream of windows with the result calculated per window.
 */
object TransactionStreamService {

  /*
    Generate a sequence of windowResult objects starting from the first transaction and finishing in the last transaction.
    @param transactions initial list of transactions
    @param windowOperation trait that encapsulates the functionality to apply aggregations and how to generate the WindowResult once all the transactions have been added to every window.
    @param slideSeconds number of seconds between windows. By default every minute is created a new window.
    @param durationSeconds duration of window. By default the duration is 5 minutes.
   */
  def analyzeTransactions[A](
    transactions: List[Transaction],
    windowOperation: WindowOperation[A],
    slideSeconds: Int = 60,
    durationSeconds: Int = 300
  ): Seq[WindowResult] = {
    val sorted = transactions.sortBy(_.date)
    if (sorted.size > 0) {
      val start = getTruncSecond(transactions.head.date)
      val end = getTruncSecond(transactions.reverse.head.date)

      // generating the list of the startTimestamp every slide period. Required to multiply by 1000 as the timestamps are in milliseconds.
      val startTimestamps = (start to end) by (slideSeconds * 1000)
      //generating the list of windows from the previous startTimestamp. Initializing the WindowAggregation with the empty operation.
      val windows = startTimestamps.map(ts => WindowAggregation(ts, ts + durationSeconds * 1000, windowOperation.empty))

      //iterating over the sorted transactions and having as initial value the initial windows with empty aggregated values.
      val aggregatedWindows = sorted.foldLeft(windows) { (resultList, transaction) =>
        val transactionTs = transaction.date.getTime()
        //Using the collections span operation to chop the sequence between the already processed windows and the remaining windows to process. We do this to improve performance.
        val (alreadyProcessedWindows, remainingWindows) = resultList.span(w => w.startTs < transactionTs && w.endTs < transactionTs)
        //Now we are chopping the remaining Windows between the windows associated with the current transaction and the future windows that we are not interested at this moment.
        val (windowsToAddTransaction, futureWindows) = remainingWindows.span(w => w.startTs <= transactionTs && transactionTs < w.endTs)
        //We need to aggregate the transaction to the windows that it can belong to.
        val currentWindowsModified = windowsToAddTransaction.map(w => windowOperation.aggregateFunc(w, transaction))
        //Aggregating the three sequences
        alreadyProcessedWindows ++ currentWindowsModified ++ futureWindows
      }
      //Once we have a sequence of aggregated windows we need to iterate and apply the result function
      aggregatedWindows.map(w => windowOperation.resultFunc(w))
    } else {
      Seq.empty[WindowResult]
    }
  }

}
