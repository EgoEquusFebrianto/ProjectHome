package Basic
import scala.collection.mutable

object CollectionsDataType {
    def main(args: Array[String]): Unit = {
        val arr = Array(1, "2", true, null, 3.0) // Array can only update elemen
        arr(1) = 2
        println(arr.mkString(", "))

        // Immutable Collections; can not add, update, delete
        // the time complexity of data access in this collection is O(n)
        val listVar = List(1, "2", true, null, 3.0)
        val listVarEmpty = List.empty[Int]
        //    listVar(0) = "1" // Error
        val newListVar = 1 :: listVar
        println(listVar)
        println(newListVar)
        println(listVarEmpty)

        val mapVar: Map[String, Any] = Map("a" -> 1, "b" -> "AK 47")
        val mapVarEmpty = Map.empty[String, Any]

        println(mapVar)
        println(mapVarEmpty)

        val setVar = Set(1, 2, 3, 4, 4)
        val setVarEmpty = Set.empty[Int]

        println(setVar)
        println(setVarEmpty)

        val vecVar = Vector(10, 20, 30, 30)
        val vecVarEmpty = Vector.empty[Int]
        val vecVarNew = vecVar :+ 40
        val vecVarNew2 = 5 +: vecVar

        println(vecVar)
        println(vecVarEmpty)
        println(vecVarNew)
        println(vecVarNew2)
        println(vecVar.tail)
        println(vecVar.init)
        println(vecVar.head)
        println(vecVar.patch(1, Nil, 2))

        val seqVar = Seq(5, 6, 7) // Abstract, can be List or Vector
        val seqVarEmpty = Seq.empty[Object]

        println(seqVar)
        println(seqVarEmpty)

        // Mutable Collections

        val mutableArray = mutable.ArrayBuffer("1", 2, 1, 3)
        mutableArray.prependAll(Seq(0, 1))
        mutableArray += 1 // add
        mutableArray -= 1 // remove
        mutableArray(0) = 4 // update
        mutableArray.remove(mutableArray.length - 1) // remove by index
        println(mutableArray)

        val mutableList = mutable.ListBuffer(1, 2, 3)
        0 +=: mutableList
        mutableList += 4
        mutableList -= 2
        println(mutableList)

        val mutableSet = mutable.Set(1, 2, 2, 3)
        mutableSet += 4
        mutableSet -= 3
        println(mutableSet)

        val mutableMap = mutable.HashMap("a" -> 1, "b" -> 2)
        mutableMap("c") = 3
        mutableMap("a") = 10
        mutableMap -= "b"
        println(mutableMap)

        
    }
}
