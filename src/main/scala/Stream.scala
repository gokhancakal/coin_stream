package cakal.lab

import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Stream {

  val coinList = List("bitcoin","ripple","cardano")
  val currList = List("usd")

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Coin-Stream").setMaster("local[5]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Minutes(1))
    ssc.sparkContext.setLogLevel("ERROR")

    val util = new Utils()

    val coinURL = util.getCoinURL(coinList, currList)

    for (((coin,curr),url) <- coinURL){
      println(url)
      val data = ssc.receiverStream(new CoinReceiver(url))
      data.foreachRDD(rdd => {
        val data = util.getCoinData(sqlContext, rdd, coin, curr)
        data.show(truncate = false)
      })
    }
    ssc.start()
    ssc.awaitTermination()
  }
}