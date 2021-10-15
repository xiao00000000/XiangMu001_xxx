package XiangMu01.LianJieGongJu

//定义一个特性
trait TagTrait {
  def makeTage(args: Any*): Map[String, Int]
}


//trait 是一种概念
//可以被当作一个接口来使用。

//同时可以定义抽象方法，与抽象类里的抽象方法一样，不给出方法的具体实现。
