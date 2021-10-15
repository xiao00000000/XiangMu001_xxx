package XiangMu01.FangFaLei

import JiekouLei.TagTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

//需求2：App名称
object AppTags extends TagTrait {
  override def makeTage(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row] //输入第一个值
    val broadcastAppname: Map[String, String] = args(1).asInstanceOf[Map[String, String]]

    //获取appid appname
    val appid: String = row.getAs[String]("appid")
    val appname: String = row.getAs[String]("appname")


    //获取渠道
    val adplatformproviderid: Int = row.getAs[Int]("adplatformproviderid")

    if (StringUtils.isEmpty(appname)) {
      if (broadcastAppname.contains(appid)) {
        map += "APP" + broadcastAppname.getOrElse(appid, "未知") -> 1
      }
    } else {
      map += "APP" + appname -> 1
    }
    map += "CN" + adplatformproviderid -> 1

    map
  }

}
