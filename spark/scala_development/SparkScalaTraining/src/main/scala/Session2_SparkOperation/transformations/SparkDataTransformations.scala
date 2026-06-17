package Session2_SparkOperation.transformations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDataTransformations {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark App")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      (1, "Andi", 5000),
      (2, "Budi", 7000),
      (3, "Citra", 6000),
      (4, "Dedi", 8000),
      (3, "Citra", 6000),
      (1, "Andi", 5000)
    ).toDF("id", "name", "salary")

    val ds = Seq(1,2,3,4,5).toDS()

    // <**> Row Transformation <**>
    // transformation 1: filter
    // With Column Object Expression
//    val res_coe = df.filter(col("salary") > 6000)

    // With SQL/String Expression
//    val sql = s"salary > 6000"
//    val res_se = df.filter(sql)

    // transformation 2: limit
//    val res_coe = df.limit(3)

    // transformation 3: distinct
//    val res_coe = df.distinct()

    // transformation 4: dropDuplicates(col), when col is null, will drop duplicates from all columns
//    val res_coe = df.dropDuplicates()

    // <**> Column Transformation <**>
    // transformation 5: select
    // With Column Object Expression
//    val res_coe = df.select(
//      col("id"),
//      col("name"),
//      (col("salary") * 1.1).alias("salary_increased"),
//      when(col("salary") > 6000, lit("High")).otherwise(lit("Low")).alias("salary_category")
//    )
//
//    // With SQL/String Expression
//    val res_se = df.selectExpr(
//      "id",
//      "name",
//      "salary * 1.1 as salary_increased",
//      "case when salary > 6000 then 'High' else 'Low' end as salary_category"
//    )

    // transformation 6: withColumn
    // With Column Object Expression
    

    // With SQL/String Expression

    // transformation
    // With Column Object Expression

    // With SQL/String Expression

    res_coe.show()
    res_se.show()
  }
}
