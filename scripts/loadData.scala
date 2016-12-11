import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions.{array, lit, map, struct}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel._

// Constants
val dataFilePath = "/home/vagrant/data/lse/lse_all.txt"
val responseColumnName = "price_change_percent"
val numberOfDaysPerWeek = 5 // LSE is weekdays only

// Parameters
val maxPastOffset = 30
val predictionOffset = 5
val dateIndexSkipStep = numberOfDaysPerWeek - 1 // Note: should be considered relative to the week length, to avoid favouring particular days
val dateIndexSkip = 1//((maxPastOffset + predictionOffset) / dateIndexSkipStep) * dateIndexSkipStep // Note: integer division, not floating point
val maxConsideredPercentageChange = 300
val dataSplitFractions = Array(0.6, 0.2, 0.2)

val learningOffsets = (1 to maxPastOffset)//.map(i => i * -1)

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
  val dateRDD = data.select(dateColumnName).distinct().orderBy(col(dateColumnName)).rdd.map {
    case Row(date: String) => (date)
  }.sortBy(i => i).zipWithIndex.map(r => Row(r._1, r._2))

  val dateSchema =
    StructType(
      StructField(dateColumnName, StringType, false) ::
        StructField("index", LongType, false) :: Nil)
  spark.createDataFrame(dateRDD, dateSchema)
}

def buildTickerIndex(data: Dataset[Row], tickerColumnName: String): Dataset[Row] = {
  val tickerRDD = data.select(tickerColumnName).distinct().rdd.map {
    case Row(ticker: String) => (ticker)
  }.sortBy(i => i).zipWithIndex.map(r => Row(r._1, r._2))

  val tickerSchema =
    StructType(
      StructField("ticker", StringType, false) ::
        StructField("ticker_index", LongType, false) :: Nil)
  spark.createDataFrame(tickerRDD, tickerSchema)
}

def difference(a: Float, b: Float):Float = {
  b - a
}

def percentChange(originalValue: Float, newValue: Float):Float = {
  ((newValue - originalValue) / originalValue) * 100
}

val differenceUdf = udf(difference(_:Float,_:Float):Float)
val percentChangeUdf = udf(percentChange(_:Float, _:Float):Float)
val window = Window.partitionBy("ticker").orderBy("index")

def addOffset(dataToAddTo: Dataset[Row], originalData: Dataset[Row], offset: Int): Dataset[Row] = {
  dataToAddTo.withColumn("close" + offset.toString, lit(0f)).withColumn("volume" + offset.toString, lit(0))
}

def combineWithOffsets(data: Dataset[Row], offsets: Seq[Int]): Dataset[Row] = {
  offsets.foldLeft(data)((d, o) => (
    d
    withColumn("close-" + o.toString, lag(col("close"), o).over(window))
    withColumn("volume-" + o.toString, lag(col("volume"), o).over(window))
  ))
}

val stockDF = loadMetastock(dataFilePath)
stockDF.createOrReplaceTempView("stocks")

val dateDF = buildDateIndex(stockDF, "date")
dateDF.createOrReplaceTempView("dates")

val tickerDF = buildTickerIndex(stockDF, "ticker")
tickerDF.createOrReplaceTempView("tickers")

var allTickers = spark.sql("SELECT DISTINCT ticker FROM stocks")

val stockWithIndexDF = spark.sql("SELECT index, ticker_index as ticker, close, volume FROM stocks JOIN dates ON stocks.date = dates.date JOIN tickers ON stocks.ticker = tickers.ticker")
val filteredDF = stockWithIndexDF//.where("ticker = 'GSK' OR ticker = 'HIK' OR ticker = 'OML'")//.join(filteredTickers, List("ticker"), "inner")//.where("index > 100").where("index < 500")
filteredDF.persist(MEMORY_AND_DISK)
val joinedData = combineWithOffsets(filteredDF, learningOffsets).na.drop().filter($"index" % dateIndexSkip === 0)
joinedData.show()

val dataForCalculationResponses = filteredDF.select("index", "ticker", "close")
val possibleResponses = (
  dataForCalculationResponses
    withColumn("future_price", lag(col("close"), -predictionOffset).over(window))
    withColumn("price_difference", differenceUdf(col("close"), col("future_price")))
    withColumn(responseColumnName, ($"price_difference" / $"close") * 100)
    drop("future_price")
    drop("price_difference")
  )

joinedData.show()
possibleResponses.show()

// Align training data (X) and expected responses (Y) ready for passing to model
// Because of offsets for past and future data, they cover different time-periods: find the intersection
val allData = (
  joinedData
    withColumn(responseColumnName, (differenceUdf(col("close"), lag(col("close"), -predictionOffset).over(window)) / $"close") * 100)
    drop("index")
    where(col(responseColumnName) < maxConsideredPercentageChange)
  //  drop("ticker")
  ).na.drop() // Drop nulls due to windowing/sliding
allData.cache()//.persist(MEMORY_AND_DISK)
//allData.show()
//allData.count()
//allData.printSchema()

val splits = allData.randomSplit(dataSplitFractions)
val (trainingData, testData, crossValidationData) = (splits(0), splits(1), splits(2))