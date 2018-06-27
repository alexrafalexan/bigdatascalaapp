import magellan.{Point, Polygon, PolyLine}
import magellan.{Point, Polygon}
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import scala.io.StdIn.readLine
// import magellan.coord.NAD83
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

val neighborhoods = sqlContext.read.format("magellan").
load("/usr/lib/spark/projects/polygon/Beijing").
select($"polygon", $"metadata").
cache()
