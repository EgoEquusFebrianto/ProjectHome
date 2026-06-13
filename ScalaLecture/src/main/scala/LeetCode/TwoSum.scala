package LeetCode

import scala.collection.mutable

object TwoSum {
  def Solution(data: Array[Int], target: Int): (Int, Int) = {
    val hashMap = mutable.HashMap.empty[Int, Int]
    var index:Int = 0

    for (num <- data) {
      hashMap.getOrElseUpdate(num, index)
      index += 1
    }

    index = 0
    for (num <- data) {
      var desired = target - num
      if (data.contains(desired) && hashMap(desired) != index) {
        return (index, hashMap(desired))
      }
      index += 1
    }

    (-1, -1)
  }

  def main(args: Array[String]): Unit = {
    val data:Array[Int] = Array(2, 3, 7, 7, 6)
    val target: Int = 9

    val result = Solution(data, target)
    println(result)
  }
}
