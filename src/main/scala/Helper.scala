import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.common.operators.base.JoinOperatorBase
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._

object Helper {

  case class Data(date: String, productId: String, eventName: String, userId: String)

  /**
    * Prepare Dataset From Given Input CSVFile
    *
    * @param env ExecutionEnvironment
    * @param inputPath String
    * @return DataSet[Data]
    */
  def prepareDataset(env: ExecutionEnvironment, inputPath: String): DataSet[Data] = {
    val eventsCSV = env.readTextFile(inputPath)
    eventsCSV.map {
      row =>
        val splittedRow = row.split('|')
        Data(splittedRow(0), splittedRow(1), splittedRow(2), splittedRow(3))
    }
  }

  /**
    * Unique Product View counts by ProductId
    * @param dataset DataSet[Data]
    * @return AggregateDataSet[(String, Int)]
    */
  def uniqueViewCounts(dataset: DataSet[Data]): AggregateDataSet[(String, Int)] = {
    val viewDataset = dataset.filter(data => data.eventName == "view")
    viewDataset.map {
      data =>
        (data.productId, 1)
    }.groupBy(0).sum(1)
  }

  /**
    * Unique Event counts
    * @param dataset DataSet[Data]
    * @return DataSet[(String, Int)]
    */
  def uniqueEventCounts(dataset: DataSet[Data]): DataSet[(String, Int)] = {
    dataset.groupBy(_.eventName).reduceGroup {
      (subDataset, collector: Collector[(String, Int)]) =>
        var key: String = null
        var count = 0

        for (data <- subDataset) {
          key = data.eventName
          count += 1
        }
        collector.collect((key, count))
    }
  }

  /**
    * Users Event Counts
    * @param dataset DataSet[(String, String)]
    * @return DataSet[(String, Int)]
    */
  def userEventCounts(dataset: DataSet[(String, String)]): DataSet[(String, Int)] = {
    dataset.groupBy(_._1).reduceGroup {
      (subDataset, b: Collector[(String, Int)]) =>
        var key: String = null
        var count = 0
        for (data <- subDataset) {
          key = data._1
          count += 1
        }
        b.collect((key, count))
    }
  }

  /**
    * Takes user event counts and finds Top 5 Users who fulfilled all the events
    * @param dataset DataSet[Data]
    * @return
    */
  def usersWhoHaveAllEvents(dataset: DataSet[Data]): DataSet[String] = {
    val userAndEvents = dataset.map {
      data =>
        (data.userId, data.eventName)
    }.distinct

    val usersWhoFullfillAllEvents = userEventCounts(userAndEvents).filter(data => data._2 > 3)

    val userCounts = dataset.map {
      data =>
        (data.userId, 1)
    }.groupBy(0).sum(1)

    // Take users who fullfill al the events and join with user count dataset to find top 5 users.
    val joinedDataset = userCounts
      .join(usersWhoFullfillAllEvents)
      .where(data=> data._1)
      .equalTo(data=> data._1)

    joinedDataset.sortPartition(data => data._1._2, Order.DESCENDING)
      .first(5)
      .map{
      user => user._2._1
    }
  }

  /**
    * Product Views of Given UserId
    * @param dataset DataSet[Data]
    * @param userId String
    * @return DataSet[String]
    */
  def productViewsOfUser(dataset: DataSet[Data], userId: String): DataSet[String] = {
    dataset.filter(_.userId == userId).map {
      data =>
        data.productId
    }.distinct()

  }
}
