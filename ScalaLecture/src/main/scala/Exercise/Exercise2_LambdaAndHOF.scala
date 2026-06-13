package Exercise

object Exercise2_LambdaAndHOF {
  def calculate(x: Int, y: Int, op: (Int, Int) => Int): Int = op(x,y)

  def makeMultiplier(factor: Int): Int => Int = {
    def triple(multiplier: Int): Int = factor * multiplier
    triple
  }

  def main(args: Array[String]): Unit = {
    val square = (x: Int) => x * x
    val isEven = (x: Int) => if (x % 2 == 0) true else false
    val add = (x: Int, y: Int) => x + y
    val mul = (x: Int, y: Int) => x * y
    val res = makeMultiplier(3)

    val numbers = List(1,2,3,4,5,6)
    val result = numbers.filter(x => x % 2 == 0).map(x => x * x).reduce(_ + _)
    
    println(result)
    println(res(5))
    println(calculate(3, 4, add))
    println(calculate(3, 4, mul))
    println(isEven(5))
    println(square(5))
  }
}
