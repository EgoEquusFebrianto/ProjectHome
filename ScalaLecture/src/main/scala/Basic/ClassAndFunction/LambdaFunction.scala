package Basic.ClassAndFunction

import scala.collection.mutable.ArrayBuffer

object LambdaFunction {
  def multiple_operation(a: Int, b: Int, op: (Int, Int) => Int): Int = op(a, b)

  def main(args: Array[String]): Unit = {
    val numbers: ArrayBuffer[Int] = ArrayBuffer(1,2,3,4,5)

    // Lambda Dasar 1 paramater
    val square = x => Math.pow(x, 2)
    var tmp = square(3)

    println(s"Hasil dari Lambda Function 1 parameter: $tmp")

    // Lambda Dasar 2 atau lebih paramter
    val addition = (x: Int, y: Int) => x + y
    var tmp2 = addition(2, 3)

    println(s"Hasil dari Lambda Function 2 paramter: $tmp2")

    // Lambda Multiple Expression

    val greet: String => String = (name: String) => {
      val greeting = s"Hello $name"
      greeting
    }

    // Lambda in High Order Function

    val result = multiple_operation(3, 4, (a, b) => a * b)
    println(result)
  }
}
