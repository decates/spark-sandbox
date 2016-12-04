val a = 5
val b = 3
val c = a + b

import org.apache.spark.sql._
import org.apache.spark.sql.types._

val dataFilePath = "/home/vagrant/data/lse/LSE*.txt"

def loadMetastock(dataFilePath: String): Dataset[Row] = {
  val metaStockSevenDailySchema =
    StructType(
      StructField("ticker", StringType, false) ::
      StructField("date", StringType, false) ::
      StructField("open", FloatType, false) ::
      StructField("high", FloatType, false) ::
      StructField("low", FloatType, false) ::
      StructField("close", FloatType, false) ::
      StructField("volume", IntegerType, false) :: Nil)
  val rowRDD = spark.sparkContext.textFile(dataFilePath).map(_.split(",")).map(i => Row(i(0), i(1), i(2).toFloat, i(3).toFloat, i(4).toFloat, i(5).toFloat, i(6).trim.toInt))
  spark.createDataFrame(rowRDD, metaStockSevenDailySchema)
}

def buildDateIndex(data: Dataset[Row], dateColumnName: String): Dataset[Row] = {
  val dateRDD = data.select("date").distinct().orderBy($"date").rdd.map {
       case Row(date: String) => (date)
     }.sortBy(i => i).zipWithIndex.map(r => Row(r._1, r._2))

  val dateSchema =
    StructType(
      StructField("date", StringType, false) ::
      StructField("index", LongType, false) :: Nil)
  spark.createDataFrame(dateRDD, dateSchema).limit(10)
}

val stockDF = loadMetastock(dataFilePath)
stockDF.createOrReplaceTempView("stocks")
//stockDF.show()

val dateDF = buildDateIndex(stockDF, "date")
dateDF.createOrReplaceTempView("dates")
//dateDF.show()

spark.sql("SELECT close, index FROM stocks JOIN dates ON stocks.date = dates.date WHERE ticker = 'MARS'").show()