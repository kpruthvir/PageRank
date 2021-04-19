import org.apache.spark.SparkConf
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

case class Metric(metricName: String, value: Double)

object Tweets {
  def main(args: Array[String]) {
    val spark_conf = new SparkConf()
      .setAppName("Tweets")//.setMaster("local[1]") // to run locally uncomment setMaster
    val spark = SparkSession
      .builder()
      .config(spark_conf)
      .getOrCreate()
    import spark.implicits._
    if (args.length != 2) {
      println("Usage: Tweets InputFile OutputDir")
      spark.stop()
    }
    // create Spark context with Spark configuration
    //    val spark = new SparkContext(new SparkConf().setAppName("Spark PageRank"))
    val inputFilePath = args(0)
    // 1.a) Loading the text file with headers
    val tweets_df = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv(inputFilePath)

    // 1.b) clean the empty and null values from the text
    val new_tweets_df = tweets_df.filter(($"text" =!= "") || ($"text" =!= null))
    // COMMAND ----------

    // 2) Split the data randomly
    val data_split = new_tweets_df.randomSplit(Array(0.7, 0.3), seed = 7)
    val training_data = data_split(0)
    val testing_data = data_split(1)

    // COMMAND ----------

    // 3) Build transformers, annotators, estimators, and run them through a pipeline
    import org.apache.spark.ml.Pipeline
    import org.apache.spark.ml.classification.LogisticRegression
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
    import org.apache.spark.ml.feature.{HashingTF, StopWordsRemover, StringIndexer, Tokenizer}
    import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val filtered_words = new StopWordsRemover()
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("filtered_no_stop_words")
    val hashing_tf = new HashingTF()
      .setNumFeatures(500)
      .setInputCol(filtered_words.getOutputCol)
      .setOutputCol("features")
    val indexer = new StringIndexer()
      .setInputCol("airline_sentiment")
      .setOutputCol("label")
    val lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, filtered_words, hashing_tf, indexer, lr))


    // COMMAND ----------

    // 4) Build a paramGrid:
    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashing_tf.numFeatures, Array(10, 100, 1000))
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()


    // COMMAND ----------

    // 5) Build CrossValidator model
    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator) // RegressionEvaluator
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10) // 10 is ideal
      .setParallelism(2) // Evaluate up to 2 parameter settings in parallel

    // COMMAND ----------

    // 6) Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(training_data)

    // COMMAND ----------

    // 7)
    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    val predicted_results = cvModel.bestModel.transform(testing_data)
    val prediction_vs_label = predicted_results
      .select("label", "prediction").rdd
      .map { case Row(label: Double, prediction: Double) => (prediction, label) }
    //    display(prediction_vs_label)

    // COMMAND ----------


    // Instantiate metrics object
    val metrics = new MulticlassMetrics(prediction_vs_label)

    // Confusion matrix
    println("Confusion matrix:")
    println(metrics.confusionMatrix)

    // Overall Statistics
    val accuracy = metrics.accuracy
    println("Summary Statistics")
    println(s"Accuracy = $accuracy")

    // Weighted stats
    val metric1_result = new Metric("Weighted precision:", metrics.weightedPrecision)
    val metric2_result = new Metric("Weighted recall:", metrics.weightedRecall)
    val metric3_result = new Metric("Weighted F1 score:", metrics.weightedFMeasure)
    val metric4_result = new Metric("Weighted false positive rate:", metrics.weightedFalsePositiveRate)
    val metric5_result = new Metric("Weighted false positive rate:", metrics.weightedTruePositiveRate)

    val metrics_DF: DataFrame = Seq(metric1_result, metric2_result, metric3_result, metric4_result, metric5_result).toDF()
    //matrices.saveAsTextFile(args(1))
    // 9)
    // Write the data from the metrics as csv type to S3 bucket output file
    metrics_DF.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .save(args(1))


    spark.stop()
  }
}
