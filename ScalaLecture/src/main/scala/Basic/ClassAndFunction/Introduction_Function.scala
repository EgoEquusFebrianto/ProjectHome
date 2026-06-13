package Basic.ClassAndFunction

import scala.math._

object Introduction_Function {
  def Operation(a: Int, b: Int) = {
    var figure1 = sqrt(a)
    var figure2 = sqrt(b)

    var res: Any = figure1 + figure2
    res
  }

  def main(args: Array[String]): Unit = {
    println(Operation(4, 9))
  }
}