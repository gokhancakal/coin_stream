package cakal.lab

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{col, from_unixtime, lit}


class Utils() {

  def formatCoinData(SQLContext: SQLContext, data: RDD[String], coin: String, curr: String): DataFrame = {
    val dF = SQLContext.read.format("json")
      .option("inferSchema", "true")
      .option("multiLine", "true")
      .json(data)

    val dF2 = dF.withColumn("Crypto Currency", lit(coin))
      .withColumn("Currency", lit(curr))
      .withColumn("Price", col(coin + "." + curr))
      .withColumn("24H Volume", col(coin + "." + curr + "_24h_vol"))
      .withColumn("24H Change", col(coin + "." + curr + "_24h_change"))
      .withColumn("Market Cap", col(coin + "." + curr + "_market_cap"))
      .withColumn("Date", from_unixtime(col(coin + ".last_updated_at"), " HH:mm:ss dd-MM-yyyy"))

    val dF3 = dF2.select(col("Crypto Currency"), col("Currency"), col("Price"),
      col("24H Volume"), col("24H Change"), col("Market Cap"),
      col("Date"))
    dF3
  }

  def getCoinData(SQLContext: SQLContext, data: RDD[String], coinList: String, currList: String): DataFrame = {

    import SQLContext.implicits._

    val colSeq = Seq("Crypto Currency", "Currency", "Price", "24H Volume", "24H Change", "Market Cap", "Date")
    var dF = Seq.empty[(String, String, String, String, String, String, String)].toDF(colSeq: _*)
    val dF2 = formatCoinData(SQLContext, data, coinList, currList)
    dF = dF.union(dF2)
    dF
  }

  def getCoinURL(coin: List[String], curr: List[String]): List[(String, String)] = {

    var urlList = List[String]()
    var coinList = List[String]()
    val mList = curr.flatMap(x => coin.map(y => (x, y)))
    for ((currency, coin) <- mList) {
      val url = "https://api.coingecko.com/api/v3/simple/price?ids=" + coin + "&vs_currencies=" + currency + "&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"
      urlList = urlList :+ url
      coinList = coinList :+ coin

    }
    val mergedList = coinList zip urlList
    mergedList
  }
}