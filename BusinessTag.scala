package XiangMu01.FangFaLei

import JiekouLei.TagTrait
import ch.hsr.geohash.GeoHash
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis
import utils.jedisUtil

object BusinessTag extends TagTrait {
  override def makeTage(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    val row: Row = args(0).asInstanceOf[Row]

    val longitude: String = row.getAs[String]("long")
    val lat: String = row.getAs[String]("lat")

//
//    var Business: String = ""
//    if (StringUtils.isNotEmpty(longitude) && StringUtils.isNotEmpty(lat) && longitude.equals("")) {
//      val Business01: String = SNTools.getBusiness(lat + "," + longitude)
//      Business = Business01
//    }


    //调用百度SNTools


    if (lat.toDouble > 3 && lat.toDouble < 54 && longitude.toDouble > 73 && longitude.toDouble < 136) {
      val code: String = GeoHash.withCharacterPrecision(lat.toDouble, longitude.toDouble, 8).toBase32
      val jedis: Jedis = jedisUtil.getJedis
      val business: String = jedis.get(code)

      map += "SN" + business -> 1
        jedis.close()
       }
        map
       }



    }

