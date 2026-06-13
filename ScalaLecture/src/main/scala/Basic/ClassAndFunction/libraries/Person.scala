package Basic.ClassAndFunction.libraries

class Person(val name: String, val favoriteThing: String) {
  def introduce(): Unit = {
    println(s"Hallo my name is $name, my Favorite Thing is $favoriteThing")
  }
}