package XiangMu01.FangFaLei

import JiekouLei.TagTrait
import org.apache.spark.sql.Row

object KeyTags extends TagTrait {
  override def makeTage(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]

    //停用词广播变量
    val boroadcastStopword: Map[String, Int] = args(1).asInstanceOf[Map[String, Int]]

    //获取字段
    val keywords: String = row.getAs[String]("keywords")

    keywords.split("\\|").filter(ke => ke.length >= 3 && ke.length <= 8 && !boroadcastStopword.contains(ke)).foreach(au => map += "K" + au -> 1)

    map
  }
}
