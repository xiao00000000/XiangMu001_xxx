package XiangMu01.QingXiShuJu

import QiangMu01.DiEerZhong001.ZhuanHuanLei002

class Log001( //传入的一整行数据
                       val sessionid: String,
                       val advertisersid: Int,
                       val adorderid: Int,
                       val adcreativeid: Int,
                       val adplatformproviderid: Int,
                       val sdkversion: String,
                       val adplatformkey: String,
                       val putinmodeltype: Int,
                       val requestmode: Int,
                       val adprice: Double,
                       val adppprice: Double,
                       val requestdate: String,
                       val ip: String,
                       val appid: String,
                       val appname: String,
                       val uuid: String,
                       val device: String,
                       val client: Int,
                       val osversion: String,
                       val density: String,
                       val pw: Int,
                       val ph: Int,
                       val longitude: String,
                       val lat: String,
                       val provincename: String,
                       val cityname: String,
                       val ispid: Int,
                       val ispname: String,
                       val networkmannerid: Int,
                       val networkmannername: String,
                       val iseffective: Int,
                       val isbilling: Int,
                       val adspacetype: Int,
                       val adspacetypename: String,
                       val devicetype: Int,
                       val processnode: Int,
                       val apptype: Int,
                       val district: String,
                       val paymode: Int,
                       val isbid: Int,
                       val bidprice: Double,
                       val winprice: Double,
                       val iswin: Int,
                       val cur: String,
                       val rate: Double,
                       val cnywinprice: Double,
                       val imei: String,
                       val mac: String,
                       val idfa: String,
                       val openudid: String,
                       val androidid: String,
                       val rtbprovince: String,
                       val rtbcity: String,
                       val rtbdistrict: String,
                       val rtbstreet: String,
                       val storeurl: String,
                       val realip: String,
                       val isqualityapp: Int,
                       val bidfloor: Double,
                       val aw: Int,
                       val ah: Int,
                       val imeimd5: String,
                       val macmd5: String,
                       val idfamd5: String,
                       val openudidmd5: String,
                       val androididmd5: String,
                       val imeisha1: String,
                       val macsha1: String,
                       val idfasha1: String,
                       val openudidsha1: String,
                       val androididsha1: String,
                       val uuidunknow: String,
                       val userid: String,
                       val iptype: Int,
                       val initbidprice: Double,
                       val adpayment: Double,
                       val agentrate: Double,
                       val lomarkrate: Double,
                       val adxrate: Double,
                       val title: String,
                       val keywords: String,
                       val tagid: String,
                       val callbackdate: String,
                       val channelid: String,
                       val mediatype: Int
                     ) extends Product with Serializable {   //
  //通过索引索取字段 n:int是索引
  override def productElement(n: Int): Any = n match {
    case 0 => sessionid: String
    case 1 => advertisersid: Int
    case 2 => adorderid: Int
    case 3 => adcreativeid: Int
    case 4 => adplatformproviderid: Int
    case 5 => sdkversion: String
    case 6 => adplatformkey: String
    case 7 => putinmodeltype: Int
    case 8 => requestmode: Int
    case 9 => adprice: Double
    case 10 => adppprice: Double
    case 11 => requestdate: String
    case 12 => ip: String
    case 13 => appid: String
    case 14 => appname: String
    case 15 => uuid: String
    case 16 => device: String
    case 17 => client: Int
    case 18 => osversion: String
    case 19 => density: String
    case 20 => pw: Int
    case 21 => ph: Int
    case 22 => longitude: String
    case 23 => lat: String
    case 24 => provincename: String
    case 25 => cityname: String
    case 26 => ispid: Int
    case 27 => ispname: String
    case 28 => networkmannerid: Int
    case 29 => networkmannername: String
    case 30 => iseffective: Int
    case 31 => isbilling: Int
    case 32 => adspacetype: Int
    case 33 => adspacetypename: String
    case 34 => devicetype: Int
    case 35 => processnode: Int
    case 36 => apptype: Int
    case 37 => district: String
    case 38 => paymode: Int
    case 39 => isbid: Int
    case 40 => bidprice: Double
    case 41 => winprice: Double
    case 42 => iswin: Int
    case 43 => cur: String
    case 44 => rate: Double
    case 45 => cnywinprice: Double
    case 46 => imei: String
    case 47 => mac: String
    case 48 => idfa: String
    case 49 => openudid: String
    case 50 => androidid: String
    case 51 => rtbprovince: String
    case 52 => rtbcity: String
    case 53 => rtbdistrict: String
    case 54 => rtbstreet: String
    case 55 => storeurl: String
    case 56 => realip: String
    case 57 => isqualityapp: Int
    case 58 => bidfloor: Double
    case 59 => aw: Int
    case 60 => ah: Int
    case 61 => imeimd5: String
    case 62 => macmd5: String
    case 63 => idfamd5: String
    case 64 => openudidmd5: String
    case 65 => androididmd5: String
    case 66 => imeisha1: String
    case 67 => macsha1: String
    case 68 => idfasha1: String
    case 69 => openudidsha1: String
    case 70 => androididsha1: String
    case 71 => uuidunknow: String
    case 72 => userid: String
    case 73 => iptype: Int
    case 74 => initbidprice: Double
    case 75 => adpayment: Double
    case 76 => agentrate: Double
    case 77 => lomarkrate: Double
    case 78 => adxrate: Double
    case 79 => title: String
    case 80 => keywords: String
    case 81 => tagid: String
    case 82 => callbackdate: String
    case 83 => channelid: String
    case 84 => mediatype: Int
  }

  override def productArity: Int = 85

  override def canEqual(that: Any): Boolean = this.isInstanceOf[Log001]   //读取类,跟log方法做匹配
}


object Log001{

  //new 类需要索引
 def apply(line:Array[String]):Log001=new Log001(
   line(0),
   ZhuanHuanLei002.toInt(line(1)),
   ZhuanHuanLei002.toInt(line(2)),
   ZhuanHuanLei002.toInt(line(3)),
   ZhuanHuanLei002.toInt(line(4)),
   line(5),
   line(6),
   ZhuanHuanLei002.toInt(line(7)),
   ZhuanHuanLei002.toInt(line(8)),
   ZhuanHuanLei002.toDouble(line(9)),
   ZhuanHuanLei002.toDouble(line(10)),
   line(11),
   line(12),
   line(13),
   line(14),
   line(15),
   line(16),
   ZhuanHuanLei002.toInt(line(17)),
   line(18),
   line(19),
   ZhuanHuanLei002.toInt(line(20)),
   ZhuanHuanLei002.toInt(line(21)),
   line(22),
   line(23),
   line(24),
   line(25),
   ZhuanHuanLei002.toInt(line(26)),
   line(27),
   ZhuanHuanLei002.toInt(line(28)),
   line(29),
   ZhuanHuanLei002.toInt(line(30)),
   ZhuanHuanLei002.toInt(line(31)),
   ZhuanHuanLei002.toInt(line(32)),
   line(33),
   ZhuanHuanLei002.toInt(line(34)),
   ZhuanHuanLei002.toInt(line(35)),
   ZhuanHuanLei002.toInt(line(36)),
   line(37),
   ZhuanHuanLei002.toInt(line(38)),
   ZhuanHuanLei002.toInt(line(39)),
   ZhuanHuanLei002.toDouble(line(40)),
   ZhuanHuanLei002.toDouble(line(41)),
   ZhuanHuanLei002.toInt(line(42)),
   line(43),
   ZhuanHuanLei002.toDouble(line(44)),
   ZhuanHuanLei002.toDouble(line(45)),
   line(46),
   line(47),
   line(48),
   line(49),
   line(50),
   line(51),
   line(52),
   line(53),
   line(54),
   line(55),
   line(56),
   ZhuanHuanLei002.toInt(line(57)),
   ZhuanHuanLei002.toDouble(line(58)),
   ZhuanHuanLei002.toInt(line(59)),
   ZhuanHuanLei002.toInt(line(60)),
   line(61),
   line(62),
   line(63),
   line(64),
   line(65),
   line(66),
   line(67),
   line(68),
   line(69),
   line(70),
   line(71),
   line(72),
   ZhuanHuanLei002.toInt(line(73)),
   ZhuanHuanLei002.toDouble(line(74)),
   ZhuanHuanLei002.toDouble(line(75)),
   ZhuanHuanLei002.toDouble(line(76)),
   ZhuanHuanLei002.toDouble(line(77)),
   ZhuanHuanLei002.toDouble(line(78)),
   line(79),
   line(80),
   line(81),
   line(82),
   line(83),
   ZhuanHuanLei002.toInt(line(84))
 ) {}
}