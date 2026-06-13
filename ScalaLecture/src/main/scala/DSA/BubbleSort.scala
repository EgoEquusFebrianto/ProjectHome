package DSA

object BubbleSort {
  def main(args: Array[String]): Unit = {
    val data: Array[Int] = Array(4,1,2,6,7,5,3)

    for (i <- 0 until data.length - 1; j <- i until data.length - i - 1 ) {
      if (data(j) > data(j + 1)) {
        val temp = data(j)
        data(j) = data(j+1)
        data(j+1) = temp
      }
    }

    println(s"Sorted data: ${data.mkString(", ")}")
  }
}
