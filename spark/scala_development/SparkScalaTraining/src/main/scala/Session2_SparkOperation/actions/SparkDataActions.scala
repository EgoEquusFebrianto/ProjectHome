package Session2_SparkOperation.actions

import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object SparkDataActions {
  def main(args: Array[String]):  Unit = {
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1, "Andi", 5000),
      (2, "Budi", 7000),
      (3, "Citra", 6000),
      (4, "Dedi", 8000)
    ).toDF("id", "name", "salary")

    val ds = Seq(1,2,3,4,5).toDS()

    // action 1: show
//    df.show()

    // action 2: count
//    val count = df.count()
//    println(s"Jumlah data = ${count}")

    // action 3: collect
//    val collect = df.collect() // hati-hati bila datanya besar.
//    println(collect.mkString("Array(", ", ", ")"))

    // action 4: take(n)
//    val take = df.take(2) // Lebih aman dari collect
//    println(take.mkString("Array(", ", ", ")"))

    // action 5: head(n), n default 20
//    df.head()

    // action 6: first
//    val first = df.first()
//    println(first)

    // action 7: foreach
//    df.foreach(row => println(row)

    // action 8: foreachPartition
    // lebih optimal dari foreach saja.

//    df.foreachPartition((partition: Iterator[Row]) => {
//      println(s"partisi ke-${TaskContext.getPartitionId()}")
//      partition.foreach(row => {
//        println(row)
//      })
//    })

    // action 9: write == SparkDataSink
    // action 10: reduce (Dataset / RDD)
//    val _reduce = ds.reduce(_ + _)
//    println(_reduce)

    // action 11: summary
//    df.summary().show()

    // action 12: describe
//    df.describe().show()

    // action 13: toLocalIterator
//    val iter = df.toLocalIterator()
//    while (iter.hasNext) {
//      println(iter.next())
//    }
  }
}