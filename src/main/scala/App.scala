import org.apache.flink.api.scala._
import org.apache.flink.util.Collector

case class Data(date: String, productId: String, eventName: String, userId: String)
case class ProductCount(productId: String, count: Int)
object App {

  def main(args: Array[String]): Unit = {
    val inputSourceDirectorty = "sample.csv"

    val env = ExecutionEnvironment.getExecutionEnvironment

    val eventsCSV = env.readTextFile(inputSourceDirectorty)

    val dataset = eventsCSV.map{
      row =>
        val splittedRow = row.split('|')
        Data(splittedRow(0),splittedRow(1),splittedRow(2),splittedRow(3))
    }

    val eventFilter = dataset.filter(data => data.eventName == "view")
    val groupedView = uniqueViewCounts(dataset) // eventFilter
    val uniqueEvents = uniqueEventCounts(dataset)
    val alleventOfUser47 = uniqueEventCounts(dataset.filter(_.userId=="47"))

    val user47 = dataset.filter(_.userId=="47").map{
      data =>
        data.productId
    }.distinct()
    groupedView.print()
    println("........")
    uniqueEvents.print()
    println("........")
    alleventOfUser47.print()
    println("........")
    dataset.filter(_.userId=="47").print()
    println("........")
    user47.print()
  }

  def uniqueViewCounts(dataset: DataSet[Data]): DataSet[ProductCount] = {
    dataset.groupBy(_.productId).reduceGroup{
      (a, b: Collector[ProductCount]) =>
        var key: String = null
        var count = 0

        for (d <- a) {
          key = d.productId
          count+=1
        }
        b.collect(ProductCount(key, count))
    }
  }

  def uniqueEventCounts(dataset: DataSet[Data]): DataSet[(String, Int)] = {
    dataset.groupBy(_.eventName).reduceGroup{
      (a, b: Collector[(String, Int)]) =>
        var key: String = null
        var count = 0

        for (d <- a) {
          key = d.eventName
          count+=1
        }
        b.collect((key, count))
    }
  }
}
