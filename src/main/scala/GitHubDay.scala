import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

object GitHubDay {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()

    val sc = spark.sparkContext

    val ghLog = spark.read.json(args(0))

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count()
    val ordered = grouped.orderBy(grouped("count").desc)

    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines()
      } yield line.trim
      )
    val bcEmployees = sc.broadcast(employees)

    import spark.implicits._
    val isEmp = (user: String) => bcEmployees.value.contains(user)
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.write.format(args(3)).save(args(2))
  }
}
