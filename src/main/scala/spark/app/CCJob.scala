package spark.app

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.{SparkContext, SparkConf}
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.util.Try

/**
 * Created by zx on 16-5-30.
 */
object CCJob extends SparkJob {

  def main(args: Array[String]): Unit ={
    val config = ConfigFactory.parseString("")
    val sparkConf = new SparkConf().setAppName("PRJob")
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
      (parts(0).toInt, parts(1).toInt)
    }).groupByKey().cache()
    var ranks = firstRDD.map(eMsg => (eMsg._1, eMsg._1))
    for (i <- 1 to 3) {
      val contribs = firstRDD.join(ranks).values.flatMap{ value =>
        value._1.map(vtx => (vtx, math.min(vtx, value._2)))
      }
      ranks = contribs.reduceByKey((v1,v2) => math.min(v1, v2))
    }

    ranks.saveAsTextFile(outputPath)
    appName
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    Try(config.getString("inputPath"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("Input path is necessary."))
  }

}
