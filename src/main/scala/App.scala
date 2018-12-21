
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

case class Data(date: String, productId: String, eventName: String, userId: String)

case class ProductCount(productId: String, count: Int)

object App {

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)

    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataset = prepareDataset(env, inputPath)
    val uniqueProductViewCounts = uniqueViewCounts(dataset)
    val uniqueEvents = uniqueEventCounts(dataset)
    val user47Dataset = dataset.filter(_.userId == "47")
    val allEventsOfUser47 = uniqueEventCounts(user47Dataset)
    val userCounts = usersWhoHaveAllEvents(dataset)
    val user47Products = productViewsOfUser(dataset, "47")

    uniqueProductViewCounts.writeAsCsv("/outputs/uniqueProductEvents","\n",
      "|",FileSystem.WriteMode.OVERWRITE)
    uniqueEvents.writeAsCsv("/outputs/uniqueEventCounts","\n",
      "|",FileSystem.WriteMode.OVERWRITE)
    userCounts.writeAsCsv("/outputs/usersWhoHaveAllEvents", "\n",
      "|",FileSystem.WriteMode.OVERWRITE)
    allEventsOfUser47.writeAsCsv("/outputs/allEventsOfUser47","\n",
      "|",FileSystem.WriteMode.OVERWRITE)
//    user47Products.writeAsCsv("user47ProductViews.csv")

    env.execute()

  }

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
    * @param dataset
    * @return
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
  def usersWhoHaveAllEvents(dataset: DataSet[Data]): DataSet[(String, Int)] = {
    val userAndEvents = dataset.map {
      data =>
        (data.userId, data.eventName)
    }.distinct

    userEventCounts(userAndEvents).filter(data => data._2 > 3)
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
