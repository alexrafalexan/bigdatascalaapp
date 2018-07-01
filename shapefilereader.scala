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
import java.util.regex.PatternSyntaxException
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.broadcast

val spark = SparkSession.builder().getOrCreate()

val sqlContext= new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._
case class TaxiRecord(tid: String, timestamp: String, point: Point)

def loadfiletrajectories (): DataFrame = {

  val taxiloadfile: DataFrame = null
  var check = true
  while (check == true){
    println("Select the A file")
    val a = repl.in.readLine("Write full path:")
    println(s"$a")
    println("Select how the file split")
    val splitarg = repl.in.readLine("Write the split argument:")
    println(s"$splitarg")
    try {
      val taxiloadfile = sc.textFile(s"$a").map{line =>
        val parts = line.split(s"$splitarg")
        val tid = parts(0)
        val timestamp = parts(1)
        val point = Point(parts(2).toDouble, parts(3).toDouble)
        TaxiRecord(tid,timestamp,point)
      }.toDF()
      check = false
      taxiloadfile.show()
      return taxiloadfile
    }
    catch {
      case ex: InvalidInputException =>{
        println("DEN YPARXEI ARXEIO")
        check = true
      }
      case ex: IllegalArgumentException => {
        println("TO EISAGOMENO DN EINAI MORFIS PATH")
        check = true
      }
      case ex: Exception =>{
        println("LATHOS DELIMITER")
        check = true
      }
    }
  }
  return taxiloadfile
}
def loadfolderpolygon (): DataFrame ={
  var polygonsDF:DataFrame = null
  var check = true
  while (check == true){
    println("Select the Polygon file")
    val b = repl.in.readLine("Write full path:")
    println(s"$b")
  try {
    val polygonsDF = sqlContext.read.format("magellan").
      load(s"$b").
      select($"polygon", $"metadata").
      cache().toDF()
      check = false
      polygonsDF.show()
    return polygonsDF
  } catch {
    case ex: IOException => {
      println("Please enter correct Second file path")
    }
    case ex: IllegalArgumentException => {
      println("TO EISAGOMENO DN EINAI MORFIS PATH")
      check = true
    }
  }
}
return polygonsDF
}


val file1:DataFrame = loadfiletrajectories()
// val file2:DataFrame = loadfolderpolygon()
val file2:DataFrame = loadfiletrajectories()


// val jj: DataFrame = file2.join(file1, file2.col("tid") === file1.col("tid") && file2.col("timestamp") === file1.col("timestamp")).
// select(file1.col("tid"),file1.col("point"),file2.col("tid"),file2.col("point"))
//
// jj.show(5000)

val jj: DataFrame = file2.join(broadcast(file1),file2.col("tid") === file1.col("tid") && file2.col("timestamp") === file1.col("timestamp")).
select(file1.col("tid"),file1.col("point"),file2.col("tid"),file2.col("point"))

jj.show(5000)

// file2.join(broadcast(file1),).
// select($"tid", $"timestamp",$"point").show()
// file1.join(broadcast(file2),$"point").
//   where($"point".within($"polygon")).
//   select($"tid", $"timestamp").
//   withColumnRenamed("v", "neighborhood").drop("k").show(5)
