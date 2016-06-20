package spark.app

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import spark.jobserver.{SparkJobInvalid, SparkJobValid, SparkJobValidation, SparkJob}

import scala.util.Try

/**
 * Created by zx on 16-5-16.
 */
object SampleJob extends SparkJob{

  def main(args: Array[String]): Unit ={
    val config = ConfigFactory.parseString("")
    val sparkConf = new SparkConf().setAppName("SampleJob")
    val sparkContext = new SparkContext(sparkConf)
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
    })
    firstRDD.groupByKey().saveAsTextFile(outputPath)
    appName
  }

  override def validate(sc: C, config: Config): SparkJobValidation = {
    Try(config.getString("inputPath"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("Input path is necessary."))
  }

}
