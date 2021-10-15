package XiangMu01.CeShiLei

import JiekouLei.DiTuAPI.BusinessTag
import JiekouLei.{AdsTags, AppTags, DriverTags, KeyTags, PCTag, TagUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object AppTagCeShiLei {
  def main(args: Array[String]): Unit = {
    if (args.length != 4) {
      println(
        """
          |缺少参数
          |inputPath
          |appmapping
          |stopwords
          |outputPath
          |""".stripMargin)
      sys.exit()
    }
    var Array(inputPath, appmapping, stopwords, outputPath) = args



    //配置连接spark
    val conf: SparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setMaster("local[*]").setAppName("AppTag")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //读取数据
    val rdd: RDD[String] = sc.textFile(appmapping)
    val arr: Map[String, String] = rdd.map(lin => {
      val strings: Array[String] = lin.split("[:]", -1)
      (strings(0), strings(1))
    }).collect().toMap

    val appmapping01: Broadcast[Map[String, String]] = sc.broadcast(arr) //广播   appmapping表

    //读取排除数据表
    val rdd1: RDD[String] = sc.textFile(stopwords)
    val arr1: Map[String, Int] = rdd1.map((_, 0)).collect().toMap
    val stopwords01: Broadcast[Map[String, Int]] = sc.broadcast(arr1) //广播  排除数据表


    //读取日志表
    val DF: DataFrame = spark.read.parquet(inputPath)

    import spark.implicits._
    val zhi: Dataset[(String, List[(String, Int)])] = DF.where(TagUtil.tagUserIdFilterParam).map(row => {
      //广告标签
      val adsTags: Map[String, Int] = AdsTags.makeTage(row)
      //app标签
      val appTags: Map[String, Int] = AppTags.makeTage(row, appmapping01.value)
      // 驱动标签.
      val driverTags: Map[String, Int] = DriverTags.makeTage(row)

      // 关键字标签.
      val keyMap: Map[String, Int] = KeyTags.makeTage(row, stopwords01.value)
      // 省市标签
      val pcMap: Map[String, Int] = PCTag.makeTage(row)

      //商圈
      val shangquan: Map[String, Int] = BusinessTag.makeTage(row)

      (TagUtil.getUserId(row)(0), (adsTags ++ appTags ++ driverTags ++ keyMap ++ pcMap++shangquan).toList)
    })


    //(IMD:126C199186F224,List((ZP北京市,1), (KTV版,1), (K动漫卡通,1), (D00020005,1), (APP爱奇艺,1), (K最新更新,1), (LN视频前贴片,1), (ZC北京市,1), (CN100018,1), (D00030004,1), (K动漫大全,1), (D00010001,1), (LC12,1)))

//    val he: RDD[(String, List[(String, Int)])] = zhi.rdd reduceByKey ((lin1, lin2) => {
//      (lin1 ++ lin2).groupBy(_._1).mapValues(_.foldLeft(0)(_ + _._2)).toList
//    })


    val shuchu: RDD[(String, List[(String, Int)])] = zhi.rdd reduceByKey ((lin1, lin2) => {
      (lin1 ++ lin2).groupBy(_._1).map {
        case (key, value) => (key, value.map(_._2).sum)
      }.toList
    })

   // shuchu.foreach(println(_))



   // (IMD:B325C338895B39BDA990E9D724D5CF5D,List((KTV版,1), (D00020005,1), (K7-10岁,1), (KPPS少,1), (APP爱奇艺,1), (LN视频前贴片,1), (K少儿卡通,1), (CN100018,1), (K7-13,1), (D00030004,1), (ZC蚌埠市,1), (K动漫大全,1), (D00010001,1), (LC12,1), (ZP安徽省,1), (K年龄段,1)))
   // (ID:7087EC03-3CB9-47A9-B954-9D15460D4A98,List((D00010002,1), (ZP山东省,1), (K言情剧,1), (D00020005,1), (ZC青岛市,1), (APP爱奇艺,1), (K最新更新,1), (LC09,1), (K华语剧场,1), (LN视频暂停悬浮,1), (CN100018,1), (D00030004,1), (K谍战剧,1), (K内地剧场,1)))


    //添加图形计算  计算同一个idm 的通过id
//     shuchu.map{
//       case (userId,userTags)=>{
//         val put: Put = new Put(Bytes.toBytes(userId))   //使用userId，当作rowkey
//         val str: String = userTags.map(t => t._1 + "," + t._2).mkString(",")
//         var day="2020-09-30"
//         put addImmutable(Bytes.toBytes("str"), Bytes.toBytes(s"day${day}"),Bytes.toBytes(str))
//         (new ImmutableBytesWritable(),put)
//       }
//     }.foreach(println(_))








   // 判断文件是否存在，存在就删除
    val cf: Configuration = sc.hadoopConfiguration
    val path: Path = new Path(outputPath)
    val system: FileSystem = FileSystem.get(cf)
    if(system.exists(path)){
      system.delete(path)
    }

    shuchu.saveAsTextFile(outputPath)



  }
}
