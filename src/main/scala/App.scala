
import org.apache.flink.api.scala._
import Helper._
import org.apache.flink.api.common.operators.Order

import scala.util.Try

case class ProductCount(productId: String, count: Int)

object App {

  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val path = Try(args(1)).getOrElse("")
//    val env = ExecutionEnvironment.createRemoteEnvironment("localhost",6123,
//      "/target/scala-2.11/TChallenge-0.1.jar")
    val env = ExecutionEnvironment.getExecutionEnvironment

    val dataset = prepareDataset(env, inputPath)
    val uniqueProductViewCounts = uniqueViewCounts(dataset)
    val uniqueEvents = uniqueEventCounts(dataset)
    val user47Dataset = dataset.filter(_.userId == "47")
    val allEventsOfUser47 = uniqueEventCounts(user47Dataset)
    val userCounts = usersWhoHaveAllEvents(dataset)
    val user47Products = productViewsOfUser(dataset, "47")


    uniqueProductViewCounts.writeAsCsv(path+"/outputs/uniqueProductEvents.csv","\n",
      "|")
    println(s"Unique Product View counts by ProductId written to $path/outputs/uniqueProductEvents.csv ")


    uniqueEvents.writeAsCsv(path+"/outputs/uniqueEventCounts.csv","\n",
      "|")
    println(s"Unique Event counts written to $path/outputs/uniqueEventCounts.csv ")


    userCounts.writeAsText(path+"/outputs/usersWhoHaveAllEvents.csv")
    println(s"Top 5 Users who fulfilled all the events (view,add,remove,click) written to $path/outputs/usersWhoHaveAllEvents.csv")


    allEventsOfUser47.writeAsCsv(path+"/outputs/allEventsOfUser47.csv","\n",
      "|")
    println(s"All events of #UserId : 47 written to $path/outputs/allEventsOfUser47.csv")


    user47Products.writeAsText(path+"/outputs/user47ProductViews.csv")
    println(s"Product Views of #UserId : 47 written to $path/outputs/user47ProductViews.csv")

    user47Products.print()
  }
}
