package Basic

object ControlFlow {
  def main(args: Array[String]): Unit = {
    val name: String = "Febrian"
    var age = 20

    if (age <= 20) {
      println(s"Hi i am $name, my old is ${age + 1}")
    } else {
      println("Hello World")
    }

    var n = 0

    println("\nWhile Loop")
    while (n < 5) {
      n += 1
      println(s"value n now is = $n")
    }

    do {
      n += 1
      println(s"value n now is = $n")
    } while (n < 10)

    println("\nFor Loop basic")
    for (i <- 0 to 5) { // ini sama artinya dengan <= n
      println(s"index now for basic 1 is = $i")
    }

    for (i <- 0 until 5) { // ini sama artinya dengan < dari n
      println(s"index now for basic 2 is = $i")
    }

    println("\nLoop step")
    for (i <- 10 to 1 by -2) {
      println(s"index now for loop by step 1 is = $i")
    }

    for (i <- 1 to 10 by 2) {
      println(s"index now for loop by step 2 is = $i")
    }

    println("\nLoop with collections")
    val fruits = Array("Cherry", "Watermelon", "Banana", "Apple", "Orange")
    for (f <- fruits) {
      println(s"Name Fruit now is $f")
    }

    println("\nLoop with condition, Guard Filter")
    for ( i <- 1 to 10 if i % 2 == 0) {
      println(s"$i is even number")
    }

    println("\nNested Loop")
    for (i <- 1 to 2 ; j <- 1 to 2) {
      println(s"Coordinate index now is $i.$j")
    }

    println("\nFor-Yield")
    val squares = for (i <- 1 to 5) yield i * i
    println(squares)

    println("\nFor with Pattern Matching")
    val pairs = List((1, "one"), (2, "two"), (3, "three"))
    for((num, word) <- pairs) {
      println(s"$num is $word")
    }
  }
}
