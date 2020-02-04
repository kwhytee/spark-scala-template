package template.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class testData( 
    id1: String, 
    id2: String, 
    value: Int 
)

object Main extends InitSpark {

  def main(args: Array[String]) = {
    import spark.implicits._

    val version = spark.version
    println("SPARK VERSION = " + version)

    val inputDf = getData(spark)

    inputDf.show()

    close
  }

  def getData() ={
    Seq( 
        (1, 1, 10), 
        (1, 2, 15), 
        (2, 1, 20), 
        (2, 2, 22), 
        (2, 3, 8) 
    )
  }

  def setup(spark: SparkSession)= {
    val data = getData().map(r => sales(r._1.toString(), r._2.toString(), r._3))
    spark.createDataFrame(data) 
  }
}
