package Basic.ClassAndFunction

import Basic.ClassAndFunction.libraries.{Person, Rectangle}

object Introduction_Class {
  def main(args: Array[String]): Unit = {
    val person = new Person("Jamal", "Coffee")
    val rectangle = new Rectangle(10, 20)
    person.introduce()
    println(s"Luas Persegi Panjang: ${rectangle.area()}m^2 dengan Keliling ${rectangle.perimeter()}m")
  }
}

