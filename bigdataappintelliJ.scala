import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.io.StdIn.readLine
import java.io.IOException
import java.io.FileNotFoundException
import scala.util.control.Breaks._

object HelloWorld {

    val spark = SparkSession.builder().getOrCreate()

    var check: Boolean = false
    var filename1: String = "First"
    var filename2: String = "Second"

    def loadfile(file: String): DataFrame = {

      var check = false
      var dfa: DataFrame = null
      // Create first dataframe
      while (check == false) {
        println(s"Select the $file file:")
        val a = repl.in.readLine("Write full path:")

        println(s"$a")


        try {
          var dfa: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(s"$a")
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


    def repartition(dfname: DataFrame, file: String): DataFrame = {
      var dftemprepart: DataFrame = null
      var part: Int = dfname.rdd.partitions.size
      var check = true
      while(check == true){
        println(s"DataFrame $file has $part partitions")
        println("Do you want to repartition?(y/n)")
        var a:String = repl.in.readLine("Select answer: ")
        if (a.equals("y")) {
          println("Select repartition number")
          var b = repl.in.readLine("Number: ")
          var part = (s"$b").toInt
          var dftemprepart = dfname.repartition(part)
          println(s"You select $part partitions for $file")
          var check = false
          return dftemprepart
        }else if (a.equals("n")) {
          println(s"You select $part partitions for $file")
          var check = false
          return dfname
        } else {
          println("Eisai MALAKAS!!!!")
          var check = true
        }
      }
      return dftemprepart
    }

  def main(args: Array[String]): Unit = {

    val df1: DataFrame = loadfile(s"$filename1")
    val df2: DataFrame = loadfile(s"$filename2")

    val df1repart:DataFrame = repartition(df1, s"$filename1")
    val part1 = df1repart.rdd.partitions.size
    println(s"Partition you have selected $part1")

  }
}
