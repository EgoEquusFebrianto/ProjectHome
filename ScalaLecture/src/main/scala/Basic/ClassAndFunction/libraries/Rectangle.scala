package Basic.ClassAndFunction.libraries

class Rectangle(long: Int, side: Int) {
  def area(): Int = {
    long * side
  }

  def perimeter(): Int = {
    2 * (long + side)
  }
}
