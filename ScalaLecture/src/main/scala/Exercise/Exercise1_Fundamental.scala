package Exercise

object Exercise1_Fundamental {
  def main(args: Array[String]): Unit = {
    var var1: Int = 10
    val var2: Double = 3.1425
    val var3: String = "Saya Programmer Pemula"
    val var4: Int = 4
    val var4_references: Map[Int, String] = Map(
      1 -> "Monday",
      2 -> "Tuesday",
      3 -> "Wednesday",
      4 ->"Thursday",
      5 -> "Friday",
      6 -> "Saturday",
      7 -> "Sunday"
    )
    val data: Array[String] = Array("Head", "Body", "Arms", "Leg", "Foot")
    val test_data = "foot"

    // Q1
    var1 += 5
    println(var1)

    // Q2
    if (data.contains(test_data.capitalize)) {
      println("value is valid")
    } else {
      println("Value is not valid")
    }

    // Q3
    var4_references.get(var4) match {
      case Some(value) => println(s"Now is $value.")
      case None => println(s"Wrong input.")
    }
  }
}
