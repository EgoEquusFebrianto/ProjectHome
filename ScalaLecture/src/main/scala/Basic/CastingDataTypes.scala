package Basic

object CastingDataTypes {
  def main(args: Array[String]): Unit = {
    val intValue = 100
    val doubleValue = 123.345
    val floatValue = 45.67f

    // Casting Numeric Types dengan method
    val toDouble: Double = intValue.toDouble
    val toFloat: Float = intValue.toFloat
    val toLong: Long = intValue.toLong
    val toShort: Short = intValue.toShort
    val toByte: Byte = intValue.toByte
    val toString: String = intValue.toString

    // Casting Floating Casting to Integer
    val doubleToInt: Int = doubleValue.toInt // (truncated)
    val floatToInt:Int = floatValue.toInt
    val doubleToLong: Long = doubleValue.toLong

  }
}
