package XiangMu01.FangFaLei

import JiekouLei.TagTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row


//需求1：广告位类型
object AdsTags extends TagTrait {

  override def makeTage(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]() //定义一个map集合   把数值赋值给map，作为一个返回值

    //接收参数
    //isInstanceOf 判断对象是否为指定类的对象，如果是的话，则可以使用 asInstanceOf 将对象转换为指定类型
    val row: Row = args(0).asInstanceOf[Row]
    val adspacetype: Int = row.getAs[Int]("adspacetype") //gatAs是根据字段名读取对应的values值，如果后面有别的值，当为空就返回后面的值。
    val adspacetypename: String = row.getAs[String]("adspacetypename")

    if (adspacetype > 9) {
      map += "LC" + adspacetype -> 1
    }
    if (adspacetype < 10) {
      map += "LC0" + adspacetype -> 1
    }
    if (StringUtils.isNotEmpty(adspacetypename)) { //这的if 是当前面两个条件不满足，adspacetype为空的时候用adspacetypename代替
      map += "LN" + adspacetypename -> 1
    }
    map
  }
}


//StringUtils  是一个java.lang.string类型的对象，是jdk提供的string类型方法
//如果方法返回的是null，string返回的也是null，对null不会报异常
//方法是静态的。