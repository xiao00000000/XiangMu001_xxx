package XiangMu01.FangFaLei

import JiekouLei.TagTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


object PCTag extends TagTrait {
  override def makeTage(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]

    val provincename: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")

    if (StringUtils.isNotEmpty(provincename)) {
      map += "ZP" + provincename -> 1
    }

    if (StringUtils.isNotEmpty(city)) {
      map += "ZC" + city -> 1
    }
    map
  }
}
