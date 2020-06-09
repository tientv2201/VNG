import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.DateType
import java.time.Year
import scala.reflect.io.Directory
import java.io.File
import configurations.Configuration._

object User {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("Assignment").master("local[*]").getOrCreate()
    val year = Year.now.getValue
    val sc = spark.sqlContext

    val data = sc.read.parquet(pathToDataFile)
    data.createOrReplaceTempView("userData")
    val newDF = data.withColumn("date_register", data("registration_dttm").cast(DateType))

    //AC 1:
    val numberRegisterByDay = newDF.groupBy("date_register").count()
    //Delete if the directory is existed
    val directory = new Directory(new File(pathToRegisterByDay))
    directory.deleteRecursively()

    numberRegisterByDay
      .coalesce(1).rdd.map(_.toString().replace("[", "").replace("]", ""))
      .saveAsTextFile(pathToRegisterByDay)

    //AC2:
    val numberRegisterByGender = newDF.groupBy("gender").count()

    numberRegisterByGender
      .coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true")
      .save(pathToRegisterByGender)

    //AC3
    val numberRegisterByAge = sc.sql("select sum(case when newAge.age is null then 1 else 0 end) as other, " +
      " sum(case when newAge.age < 16 then 1 else 0 end) as lesThan16, " +
      " sum(case when newAge.age >= 16 and newAge.age <= 34  then 1 else 0 end) as from16To34, " +
      " sum(case when newAge.age >= 35 and newAge.age <= 59  then 1 else 0 end) as from35To59, " +
      " sum(case when newAge.age >= 60  then 1 else 0 end) as greaterOrEqual60 " +
      s"from (select ($year - Cast(RIGHT(birthdate, 4) as int))  as age from userData) newAge")

    numberRegisterByAge
      .coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true")
      .save(pathToRegisterByAge)
  }
}
