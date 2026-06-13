package Basic

object Pattern_Matching {
  def main(args: Array[String]): Unit = {
    val x: Any = true

    x match {
      case i: Int if i > 0 => println(s"Positive Integer $i")
      case i: Double if i > 0 => println(f"Positive Decimal $i")
      case i: String => println(s"String Line $i")
      case _ => println(s"Another input: $x")
    }

  }
}
