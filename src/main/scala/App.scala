import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

case class Data(date: String, productId: String, eventName: String, userId: String)

case class ProductCount(productId: String, count: Int)

object App {

  def main(args: Array[String]): Unit = {
    val inputPath = "sample.csv"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val eventsCSV = env.readTextFile(inputPath)

    val dataset = eventsCSV.map {
      row =>
        val splittedRow = row.split('|')
        Data(splittedRow(0), splittedRow(1), splittedRow(2), splittedRow(3))
    }

    val eventFilter = dataset.filter(data => data.eventName == "view")
    val groupedView = uniqueViewCounts(eventFilter) // dataset
    val uniqueEvents = uniqueEventCounts(dataset)
    val allEventsOfUser47 = uniqueEventCounts(dataset.filter(_.userId == "47"))

    val userAndEvents = dataset.map {
      data =>
        (data.userId, data.eventName)
    }.distinct
    val userCounts = userEventCounts(userAndEvents).filter(d => d._2 > 3)

    val user47 = dataset.filter(_.userId == "47").map {
      data =>
        data.productId
    }.distinct()

    groupedView.print()
    println("........")
    uniqueEvents.print()
    println("........")
    allEventsOfUser47.print()
    println("........")
    user47.print()
    println("........")
    userCounts.print()

  }

  def uniqueViewCounts(dataset: DataSet[Data]): AggregateDataSet[(String, Int)] = {
    dataset.map{
      data =>
        (data.productId,1)
    }.groupBy(0).sum(1)
  }

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

}
