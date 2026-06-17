package Basic.scalaOperators
import scala.collection.mutable.ArrayBuffer

object ScalaOperators {
  def main(args: Array[String]): Unit = {
    // Collection Operator

    // += (Add Assignment Operator) -> menambah 1 elemen ke variabel mutable atau menambah nilai variabel integer
    val buffer = ArrayBuffer("A")
    buffer += "B"
//    println(buffer)

    // ++= (Append Multiple Elements) -> Menambah Banyak Data dari collection lain
    val buffer1 = ArrayBuffer("A", "B")
    buffer1 ++= Seq("C, D")
//    println(buffer1)

    // :+ (Append Right) -> menambah elemen di belakang collection
    // pada collection immutable
    val buffer2_1 = ArrayBuffer("A", "B")
    val buffer2_copy = buffer2_1 :+ "C"
//    println(buffer2_copy)

    // pada collection mutable
    var buffer2_2 = ArrayBuffer("A", "B")
    buffer2_2 = buffer2_2 :+ "C"
//    println(buffer2_2)

    // +: (Prepend Left) -> Menambah elemen di depan collection
    // pada collection immutable
    val buffer3_1 = ArrayBuffer("A", "B")
    val buffer3_copy = "C" +: buffer3_1
//    println(buffer3_copy)

    // pada collection immutable
    var buffer3_2 = ArrayBuffer("A", "B")
    buffer3_2 = "C" +: buffer3_2
//    println(buffer3_2)

    // :: (Cons Operator) -> Menambah elemen ke depan collection List
    // operator ini mirip dengan "+:" namun "::" khusus untuk collection list
    val list = 0 :: List(1,2,3)
//    println(list)

    // ::: (Concatenate List) -> Menggabungkan 2 List
    val list1 = List(1,2,3) ::: List(4,5,6)
//    println(list1)

    // ++ (Merge Collection) -> Menggabungkan 2 Collection
    val res = List(1,2) ++ List(3,4)
    val res1 = Set(1,2) ++ Set(3,4)
    val res2 = Seq(1,2) ++ Seq(3,4)
    val res3 = ArrayBuffer(1,2) ++ ArrayBuffer(3,4)
  }
}
