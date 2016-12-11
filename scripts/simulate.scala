// Train a Deep Learning model
import org.apache.spark.SparkFiles
import org.apache.spark.examples.h2o._
import org.apache.spark.sql.{DataFrame, SQLContext}
import water.Key
import water.support.SparkContextSupport.addFiles

import h2oContext._
import h2oContext.implicits._

val trainFrame = h2oContext.asH2OFrame(trainingData, "training_table")

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

val predictionH2OFrame = dlModel.score(testData)('predict)
val predictionsFromModel = asRDD[DoubleHolder](predictionH2OFrame).collect.map(_.result.getOrElse(Double.NaN))

val expected = testData.select(responseColumnName)
val expectedValues = expected.collect().map(r => r(0).asInstanceOf[Double])
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
