import java.text.SimpleDateFormat
import java.util.Date

import dvirgil.transaction.{ CountWindowOperation, SumWindowOperation, TransactionStreamService }
import org.scalatest.Matchers

import org.scalatest._
import dvirgil.transaction.Domain._

class TransactionStreamServiceSpec extends WordSpec with Matchers {

  "The TransactionStreamService" should {
    val ACCOUNT_B = Account("B", "000000", "00112233")
    val ACCOUNT_A = Account("A", "000000", "00112233")

    "should return an empty list when there are no transactions" in {
      val emptyList = List.empty[Transaction]
      TransactionStreamService.analyzeTransactions(emptyList, new SumWindowOperation(3000)).size should ===(0)
    }

    "should generate one window if there is just one transaction" in {
      // 24-11-2019 00:00:17 - 24-11-2019 00:10:35
      val list = Transaction(new Date(1574553643000L), ACCOUNT_A, ACCOUNT_B, 25.5F) :: Nil
      val windowsResult = TransactionStreamService.analyzeTransactions(list, new SumWindowOperation(3000))
      windowsResult.size should ===(1)
      val expectedResult = "24/11/2019 12:00:00 - 24/11/2019 12:05:00 = false"
      windowsResult.map(a => a.prettyPrint).reduce(_ + "\n" + _) should ===(expectedResult)
    }

    "should truncate the beginning of the window streaming. For example if the first transaction is on 24-11-2019 00:00:17 the first window starts on 24-11-2019 00:00:00" in {
      // 24-11-2019 00:00:17
      val list = Transaction(new Date(1574553643000L), ACCOUNT_A, ACCOUNT_B, 25.5F) :: Nil
      val windowsResult = TransactionStreamService.analyzeTransactions(list, new SumWindowOperation(3000))
      val sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss")
      sdf.format(windowsResult.head.startTs) should ===("24/11/2019 12:00:00")
    }

    "should generate correctly the number of windows based on the first and last transaction" in {
      // 24-11-2019 00:00:17 - 24-11-2019 00:10:35
      val list = Transaction(new Date(1574553643000L), ACCOUNT_A, ACCOUNT_B, 25.5F) :: Transaction(new Date(1574554235000L), ACCOUNT_A, ACCOUNT_B, 25.5F) :: Nil
      val windowsResult = TransactionStreamService.analyzeTransactions(list, new SumWindowOperation(3000))
      windowsResult.size should ===(11)
      val expectedResult =
        """24/11/2019 12:00:00 - 24/11/2019 12:05:00 = false
          |24/11/2019 12:01:00 - 24/11/2019 12:06:00 = false
          |24/11/2019 12:02:00 - 24/11/2019 12:07:00 = false
          |24/11/2019 12:03:00 - 24/11/2019 12:08:00 = false
          |24/11/2019 12:04:00 - 24/11/2019 12:09:00 = false
          |24/11/2019 12:05:00 - 24/11/2019 12:10:00 = false
          |24/11/2019 12:06:00 - 24/11/2019 12:11:00 = false
          |24/11/2019 12:07:00 - 24/11/2019 12:12:00 = false
          |24/11/2019 12:08:00 - 24/11/2019 12:13:00 = false
          |24/11/2019 12:09:00 - 24/11/2019 12:14:00 = false
          |24/11/2019 12:10:00 - 24/11/2019 12:15:00 = false""".stripMargin
      windowsResult.map(a => a.prettyPrint).reduce(_ + "\n" + _) should ===(expectedResult)

    }

    "should aggregate the transactions using the sum operator case 1" in {
      val list = Transaction(new Date(1574553643000L), ACCOUNT_A, ACCOUNT_B, 4.5f) :: // 24-11-2019 00:00:17
        Transaction(new Date(1574553640000L), ACCOUNT_A, ACCOUNT_B, 25.5f) :: // 24-11-2019 00:00:40
        Transaction(new Date(1574553675000L), ACCOUNT_A, ACCOUNT_B, 31.0f) :: // 24-11-2019 00:01:15
        Nil
      val windowsResult = TransactionStreamService.analyzeTransactions(list, new SumWindowOperation(30))
      windowsResult.size should ===(2)
      val expectedResult =
        """24/11/2019 12:00:00 - 24/11/2019 12:05:00 = true
          |24/11/2019 12:01:00 - 24/11/2019 12:06:00 = true""".stripMargin
      windowsResult.map(a => a.prettyPrint).reduce(_ + "\n" + _) should ===(expectedResult)

    }

    "should aggregate the transactions using the sum operator case 2" in {
      val list = Transaction(new Date(1574553643000L), ACCOUNT_A, ACCOUNT_B, 4.5f) :: // 24-11-2019 00:00:17
        Transaction(new Date(1574553640000L), ACCOUNT_A, ACCOUNT_B, 25.5f) :: // 24-11-2019 00:00:40
        Transaction(new Date(1574553675000L), ACCOUNT_A, ACCOUNT_B, 25.0f) :: // 24-11-2019 00:01:15
        Nil
      val windowsResult = TransactionStreamService.analyzeTransactions(list, new SumWindowOperation(30))
      windowsResult.size should ===(2)
      val expectedResult =
        """24/11/2019 12:00:00 - 24/11/2019 12:05:00 = true
          |24/11/2019 12:01:00 - 24/11/2019 12:06:00 = false""".stripMargin
      windowsResult.map(a => a.prettyPrint).reduce(_ + "\n" + _) should ===(expectedResult)

    }

    "should aggregate the transactions using the count operator case 1" in {
      val list = Transaction(new Date(1574553643000L), ACCOUNT_A, ACCOUNT_B, 4.5f) :: // 24-11-2019 00:00:17
        Transaction(new Date(1574553640000L), ACCOUNT_A, ACCOUNT_B, 25.5f) :: // 24-11-2019 00:00:40
        Transaction(new Date(1574553675000L), ACCOUNT_A, ACCOUNT_B, 31.0f) :: // 24-11-2019 00:01:15
        Nil
      val windowsResult = TransactionStreamService.analyzeTransactions(list, new CountWindowOperation(1))
      windowsResult.size should ===(2)
      val expectedResult =
        """24/11/2019 12:00:00 - 24/11/2019 12:05:00 = true
          |24/11/2019 12:01:00 - 24/11/2019 12:06:00 = true""".stripMargin
      windowsResult.map(a => a.prettyPrint).reduce(_ + "\n" + _) should ===(expectedResult)

    }

    "should aggregate the transactions using the count operator case 2" in {
      val list = Transaction(new Date(1574553643000L), ACCOUNT_A, ACCOUNT_B, 4.5f) :: // 24-11-2019 00:00:17
        Transaction(new Date(1574553640000L), ACCOUNT_A, ACCOUNT_B, 25.5f) :: // 24-11-2019 00:00:40
        Transaction(new Date(1574553675000L), ACCOUNT_A, ACCOUNT_B, 31.0f) :: // 24-11-2019 00:01:15
        Nil
      val windowsResult = TransactionStreamService.analyzeTransactions(list, new CountWindowOperation(2))
      windowsResult.size should ===(2)
      val expectedResult =
        """24/11/2019 12:00:00 - 24/11/2019 12:05:00 = true
          |24/11/2019 12:01:00 - 24/11/2019 12:06:00 = false""".stripMargin
      windowsResult.map(a => a.prettyPrint).reduce(_ + "\n" + _) should ===(expectedResult)

    }
  }

}