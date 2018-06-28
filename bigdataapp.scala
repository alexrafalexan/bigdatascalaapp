import magellan.{Point, Polygon, PolyLine}
import magellan.{Point, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine
import magellan.coord.NAD83
import org.apache.spark.sql.magellan._
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import scala.reflect.runtime.universe._
import org.apache.spark.sql.Row
import magellan.Polygon

val spark = SparkSession.builder().getOrCreate()
val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

println("Select the A file:")

val a = repl.in.readLine("Write full path:")
println(s"$a")

case class TaxiRecord(tid: String, timestamp: String, point: Point)

val taxi = sc.textFile(s"$a").map{line =>
  val parts = line.split(",")
  val tid = parts(0)
  val timestamp = parts(1)
  val point = Point(parts(3).toDouble, parts(2).toDouble)
  TaxiRecord(tid,timestamp,point)
}.toDF()

taxi.show()

val neighborhoods = sqlContext.read.format("magellan").
load("/usr/lib/spark/scala_files/planning_neighborhoods/").
select($"polygon", $"metadata").
cache().toDF()

neighborhoods.show()

neighborhoods.join(taxi).
where($"point".within($"polygon")).
select($"tid", $"timestamp").
withColumnRenamed("v", "neighborhood").drop("k").show(5)
