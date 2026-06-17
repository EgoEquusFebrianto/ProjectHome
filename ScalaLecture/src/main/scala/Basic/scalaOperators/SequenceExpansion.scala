package Basic.scalaOperators

import scala.collection.mutable.ArrayBuffer

object SequenceExpansion {
  // tanpa sequence expression (varargs expansion)
  def sum(numbers: Int*): Int = {
    numbers.sum
  }

  def main(args: Array[String]): Unit = {
    val data = Seq(1,2,3,4)
    val res1 = sum(1,2,3,4)
//    val res2 = sum(data) // Error karena parameter yang diharapkan adalah Int*

    val res3 = sum(data: _*)
    println(res3)
  }

}
