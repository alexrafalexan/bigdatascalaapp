import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.io.StdIn.readLine
import java.io.IOException
import java.io.FileNotFoundException
import scala.util.control.Breaks._

var check: Boolean = false
var filename1: String = "First"
var filename2: String = "Second"
// var dfa: DataFrame = null

def loadfile (file: String): DataFrame = {

      var check = false
      var dfa:DataFrame = null
      // Create first dataframe
      while (check == false) {
      println(s"Select the $file file:")
      val a = repl.in.readLine("Write full path:")

      println(s"$a")


      try {
        var dfa:DataFrame = spark.read.option("header","true").option("inferSchema","true").csv(s"$a")
        println(s"$file file Loaded..")
        dfa.show(5)
      //  dfa.schema()
        check = true
          return dfa
      }
      catch {
        case ex: Exception => {
          println("File not found. Please enter correct FIRST file path")
          check = false
        }
      }

    }
      return dfa
  }


def partition(dfname: DataFrame, file: String): Int ={
  val part = dfname.rdd.partitions.size
  println(s"DataFrame $file has $part partitions")
      println("Do you want to repartition?(y/n)")
  // var a = repl.in.readLine("Select answer: ")
  // if ( a == "y") {
  //   println("Select repartition number")
  //   var b = repl.in.readLine("Number")
  // }  else if (a == "n"){
  //   println("Select repartition number")
  // } else {
  //   println("Eisai MALAKAS!!!!")
  // }
    return part
}

  val df1:DataFrame = loadfile(s"$filename1")
  val df2:DataFrame = loadfile(s"$filename2")

  partition(df1,s"$filename1")
  partition(df2,s"$filename2")
