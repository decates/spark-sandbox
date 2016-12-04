import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

val dataFilePath = "/home/vagrant/data/lse/LSE*.txt"
val learningOffsets = Seq(-1, -2, -3, -5, -10, -30)
val predictionOffset = 30

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
  spark.createDataFrame(dateRDD, dateSchema)
}

def addOffset(dataToAddTo: Dataset[Row], originalData: Dataset[Row], offset: Int): Dataset[Row] = {
  // Create a copy with an offset index
  var offsetDF = originalData.withColumn("index", originalData("index") + offset)
  // Rename the offset columns to avoid duplicate column names
  val extraColumns = offsetDF.columns.filter(c => (c != "index" && c != "ticker")).foreach(c => {
    offsetDF = offsetDF.withColumnRenamed(c, c + offset.toString)
  })
  // data("index") === offsetDF("index")
  dataToAddTo.join(offsetDF, List("index", "ticker"), "inner")
}

def combineWithOffsets(data: Dataset[Row], offsets: Seq[Int]): Dataset[Row] = {
  offsets.foldLeft(data)((d, o) => addOffset(d, data, o))
}

def priceDifference(currentPrice: Float, futurePrice: Float):Float = {
  futurePrice - currentPrice
}

val priceDifferenceUdf = udf(priceDifference(_:Float,_:Float):Float)

val stockDF = loadMetastock(dataFilePath)
stockDF.createOrReplaceTempView("stocks")
//stockDF.show()

val dateDF = buildDateIndex(stockDF, "date")
dateDF.createOrReplaceTempView("dates")
//dateDF.show()
//spark.sql("SELECT close, index FROM stocks JOIN dates ON stocks.date = dates.date WHERE ticker = 'MARS'").show()

val stockWithIndexDF = spark.sql("SELECT index, ticker, open, high, low, close, volume FROM stocks JOIN dates ON stocks.date = dates.date")//.where("ticker = 'MARS'")
val filteredDF = stockWithIndexDF//.filter("index < 100")
filteredDF.cache()
//stockWithIndexDF.show()
//stockWithIndexDF.filter("index > 30").filter("ticker = 'MARS'")
val joinedData = combineWithOffsets(filteredDF, learningOffsets)
joinedData.filter("index = 0").show()

val dataForCalculationResponses = stockWithIndexDF.select("index", "ticker", "close");
val possibleResponses = (
  addOffset(dataForCalculationResponses, dataForCalculationResponses, predictionOffset)
  withColumnRenamed("close", "original_close")
  withColumnRenamed("close" + predictionOffset, "future_close")
  withColumn("price_difference", priceDifferenceUdf($"original_close", $"future_close"))
  drop("original_close")
  drop("future_close"))
possibleResponses.show()

// Align training data (X) and expected responses (Y) ready for passing to model

// Split on ticker into training, cross-validation and test sets (if h2o doesn't do it already?)