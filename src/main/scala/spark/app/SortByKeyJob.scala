package spark.app

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkContext, SparkConf}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try

/**
 * Created by zx on 16-6-16.
 */
object SortByKeyJob extends SparkJob {

  def main(args: Array[String]): Unit ={
    val config = ConfigFactory.parseString("")
    val sparkConf = new SparkConf().setAppName("SortByKeyJob")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLocalProperty("spark.scheduler.pool", "production")
    val results = runJob(sparkContext, config)
    println("Finish application: " + results.asInstanceOf[String])
  }

  override def runJob(sc: SparkContext, jobConfig: Config): Any ={
    val inputPath = jobConfig.getString("inputPath")
    val outputPath = jobConfig.getString("outputPath")
    val appName = jobConfig.getString("appName")
    val firstRDD = sc.textFile(inputPath).map(line => {
      val parts = line.split("\\s+")
      parts
    }).flatMap(parts => parts.map(key => (key.toInt,1)))
    firstRDD.sortByKey().saveAsTextFile(outputPath)
    appName
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    Try(config.getString("inputPath"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("Input path is necessary."))
  }

}
