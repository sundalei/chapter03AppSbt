import org.apache.spark.sql.SparkSession
import scala.io.Source.fromFile

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext

    val homeDir = System.getenv("HOME")
    val input = homeDir + "/sia/github-archive/2015-03-01-0.json"
    val ghLog = spark.read.json(input)

    val pushes = ghLog.filter("type = 'PushEvent'")
    val grouped = pushes.groupBy("actor.login").count()
    val ordered = grouped.orderBy(grouped("count").desc)
    ordered.show(5)

    val empPath = homeDir + "/first-edition/ch03/ghEmployees.txt"
    val employees = Set() ++ (
      for {
        line <- fromFile(empPath).getLines()
      } yield line.trim
    )
    val bcEmployees = sc.broadcast(employees)

    import spark.implicits._
    val isEmp = (user: String) => bcEmployees.value.contains(user)
    val isEmployee = spark.udf.register("SetContainsUdf", isEmp)
    val filtered = ordered.filter(isEmployee($"login"))
    filtered.show()
  }
}
