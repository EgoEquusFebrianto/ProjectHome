package Basic.ClassAndFunction

object Higher_Order_Function {
  private def applyOperation(x: Int, y: Int, op: (Int, Int) => Int): Int = op(x, y)
  private def add(a: Int, b: Int): Int = a * b

  def main(args: Array[String]): Unit = {
    val result = applyOperation(5, 3, add)
    println(result)
  }
}
