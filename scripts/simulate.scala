import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.storage.StorageLevel._

// Constants
val dataFilePath = "/home/vagrant/data/lse/LSE*.txt"
val responseColumnName = "price_change_percent"
val numberOfDaysPerWeek = 5 // LSE is weekdays only

// Parameters
val maxPastOffset = 30
val predictionOffset = 5
val dateIndexSkipStep = numberOfDaysPerWeek - 1 // Note: should be considered relative to the week length, to avoid favouring particular days
val dateIndexSkip = ((maxPastOffset + predictionOffset) / dateIndexSkipStep) * dateIndexSkipStep // Note: integer division, not floating point

val learningOffsets = (1 to maxPastOffset).map(i => i * -1)

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

def addOffset(dataToAddTo: Dataset[Row], originalData: Dataset[Row], offset: Int): Dataset[Row] = {
  // Create a copy with an offset index
  var offsetDF = originalData.withColumn("index", originalData("index") + offset)
  // Rename the offset columns to avoid duplicate column names
  offsetDF.columns.filter(c => (c != "index" && c != "ticker")).foreach(c => {
    offsetDF = offsetDF.withColumnRenamed(c, c + offset.toString)
  })
  val combinedData = dataToAddTo.join(offsetDF, List("index", "ticker"), "inner")

  // Make price data relative to the original
  combinedData.withColumn("close" + offset.toString, percentChangeUdf($"close", col("close"+offset.toString)))
}

def combineWithOffsets(data: Dataset[Row], offsets: Seq[Int]): Dataset[Row] = {
  offsets.foldLeft(data)((d, o) => addOffset(d, data, o))
}

val stockDF = loadMetastock(dataFilePath)
stockDF.createOrReplaceTempView("stocks")

val dateDF = buildDateIndex(stockDF, "date")
dateDF.createOrReplaceTempView("dates")

val tickerDF = buildTickerIndex(stockDF, "ticker")
tickerDF.createOrReplaceTempView("tickers")

var allTickers = spark.sql("SELECT DISTINCT ticker FROM stocks")
//var filteredTickers = allTickers.sample(false, 0.01)

val stockWithIndexDF = spark.sql("SELECT index, ticker_index as ticker, close, volume FROM stocks JOIN dates ON stocks.date = dates.date JOIN tickers ON stocks.ticker = tickers.ticker")
val filteredDF = stockWithIndexDF//.where("ticker = 'GSK' OR ticker = 'HIK' OR ticker = 'OML'")//.join(filteredTickers, List("ticker"), "inner")//.where("index > 100").where("index < 500")
filteredDF.persist(MEMORY_AND_DISK)
val joinedData = combineWithOffsets(filteredDF, learningOffsets).filter($"index" % dateIndexSkip === 0)
//joinedData.filter("index = 0").show()

val dataForCalculationResponses = filteredDF.select("index", "ticker", "close");
val possibleResponses = (
  addOffset(dataForCalculationResponses, dataForCalculationResponses, predictionOffset)
  withColumn(responseColumnName, col("close" + predictionOffset))
  drop("close" + predictionOffset)
  drop("close"))
//possibleResponses.show()

// Align training data (X) and expected responses (Y) ready for passing to model
// Because of offsets for past and future data, they cover different time-periods: find the intersection
val allData = (
  joinedData
  join(possibleResponses, List("index", "ticker"), "inner")
  drop("index")
  drop("ticker")
)
allData.persist(MEMORY_AND_DISK)
allData.printSchema()

val splits = allData.randomSplit(Array(0.1, 0.05, 0.85))
val (trainingData, testData, extraData) = (splits(0), splits(1), splits(2))
// Train a Deep Learning model
import org.apache.spark.SparkFiles
//import org.apache.spark.h2o._
import org.apache.spark.examples.h2o._
import org.apache.spark.sql.{DataFrame, SQLContext}
import water.Key
import water.support.SparkContextSupport.addFiles

//import org.apache.spark.h2o._
//val h2oContext = H2OContext.getOrCreate(sc)

import h2oContext._
import h2oContext.implicits._

val trainFrame = h2oContext.asH2OFrame(trainingData, "training_table")
//val validationFrame =

import _root_.hex.deeplearning.DeepLearning
import _root_.hex.deeplearning.DeepLearningModel.DeepLearningParameters
val dlParams = new DeepLearningParameters()
dlParams._epochs = 100
dlParams._train = trainFrame
dlParams._response_column = responseColumnName
dlParams._variable_importances = true
// Create a job
val dl = new DeepLearning(dlParams, Key.make("dlModel.hex"))
val dlModel = dl.trainModel.get

val predictionH2OFrame = dlModel.score(allData)('predict)
val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

val expected = allData.select(responseColumnName)
val expectedValues = expected.collect().map(r => r(0).asInstanceOf[Float])
val comp = (expectedValues zip predictionsFromModel)
val avgError = comp.map(i => (i._2-i._1)).map(math.abs).sum / comp.length
val avgPrediction = expectedValues.map(math.abs).sum / expectedValues.length
(avgPrediction, avgError)


// TODO: DONE Prevent training on basically the same data multiple times - use 'index mod N*4' to cut down the data

// TODO: Shuffle the data before processing for Deep Learning - use .sample(false, 1.0)
// TODO: Separate out training, test and CV data: split based on time, so the test is genuinely separate to the training data.
// TODO: Reduce the dimensionality: use feature hashing (max_categorical_features) or PCA
// TODO: Use K-means clustering (or similar) to group by ticker
// TODO: Include features relating to ticker: industry, other company ratings, fundamentals, etc.
// TODO: Try changing to a classification problem: pick a percentage increase and time scale, and convert to 0 or 1.
