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
import java.io.IOException
import java.io.FileNotFoundException
import org.apache.hadoop.mapred.InvalidInputException
import java.lang.ArrayIndexOutOfBoundsException
import java.lang.IllegalArgumentException

val spark = SparkSession.builder().getOrCreate()


println("Select the A file:")
val a = repl.in.readLine("Write full path:")
println(s"$a")

println("Select how the file split")
val splitarg = repl.in.readLine("Write the split argument:")
println(s"$splitarg")

val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
case class TaxiRecord(tid: String, timestamp: String, point: Point)

def loadfile (filepath: String, splitarg2: String): DataFrame = {
val taxiloadfile: DataFrame = null
try {

val taxiloadfile = sc.textFile(s"$filepath").map{line =>
  val parts = line.split(s"$splitarg2")
  val tid = parts(0)
  val timestamp = parts(1)
  val point = Point(parts(2).toDouble, parts(3).toDouble)
  TaxiRecord(tid,timestamp,point)
}.toDF()
taxiloadfile.show()
return taxiloadfile
}
catch {
  // case ex: Exception => {
  //   println("Can't split the file")
  //        }

  case ex: InvalidInputException =>{
            println("Missing file exception")
         }
  case ex: ArrayIndexOutOfBoundsException =>{
            println("GAMO TA NEYRA")
          }
  case ex: IllegalArgumentException => {
     println("GAMO TA NEYRA 2")
        }
  // case ex: IOException => {
  //           println("IO Exception")
  //        }
}
  return taxiloadfile
}


 val taxi:DataFrame = loadfile(s"$a",s"$splitarg")
//
// taxi.show()
//
// println("Select the Polygon file:")
//
// val b = repl.in.readLine("Write full path:")
// println(s"$b")
//
// val neighborhoods = sqlContext.read.format("magellan").
// load(s"$b").
// select($"polygon", $"metadata").
// cache().toDF()
//
// neighborhoods.show()
//
// neighborhoods.join(taxi).
// where($"point".within($"polygon")).
// select($"tid", $"timestamp").
// withColumnRenamed("v", "neighborhood").drop("k").show(5)
