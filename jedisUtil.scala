package XiangMu01.QianQi.GongJuLei.utils

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool
object jedisUtil {

  //jedis 工具类    Util是工具类
   def  getJedis={
     val jedispool: JedisPool = new JedisPool(new GenericObjectPoolConfig, "localhost", 6379, 30000, null, 7)
     jedispool.getResource

   }

//  def main(args: Array[String]): Unit = {
//    val jedispool: JedisPool = new JedisPool(new GenericObjectPoolConfig, "localhost", 6379, 30000, null, 7)
//    println(jedispool)
//  }

}
