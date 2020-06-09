package configurations
import java.time.LocalDate
object Configuration {
  val date = LocalDate.now.toString
  val pathToDataFile = "./src/data/parquet"
  val pathToRegisterByDay = s"./src/data/$date/registerByDay"
  val pathToRegisterByGender = s"./src/data/$date/registerByGender"
  val pathToRegisterByAge = s"./src/data/$date/registerByAge"
}
