package org.shadowmask.engine.spark.envirement

import org.shadowmask.core.AnonymityFieldType
import org.shadowmask.engine.spark.functions.DataAnoymizeFunction.lDiversityCompute
import org.shadowmask.engine.spark.functions.StatisticFunction
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.Map


object TestParamStatisticFunction {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local")
    val sc = new SparkContext(conf)
    val sourceRdd = sc.textFile(this.getClass.getClassLoader.getResource("test2.csv").getPath)
    val dataSize = sourceRdd.count()
    val fieldSeapartor = sc.broadcast(",")

    var gMap: Map[Int, AnonymityFieldType] = Map()
    gMap += (1 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (2 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (5 -> AnonymityFieldType.SENSITIVE)

    val lRdd = lDiversityCompute(sc, sourceRdd, gMap, fieldSeapartor)

    val sensitiveInfo = StatisticFunction.countSensitiveInfo(sc, lRdd)
    val fractionRdd = StatisticFunction.convertIntToFraction(sensitiveInfo)
    val entropyRdd = StatisticFunction.convertFractionToEntropy(fractionRdd)
    val fractionStatisticResult = StatisticFunction.fractionStatistics(fractionRdd)
    val sensitiveInfoResult = StatisticFunction.fractionStatistics(sensitiveInfo)
    val calculateEntropyResult = StatisticFunction.calculateEntropy(entropyRdd)
    val entropyMaxResult = StatisticFunction.calculateMax(calculateEntropyResult)
    val entropyMinResult = StatisticFunction.calculateMin(calculateEntropyResult)
    val entropyMeanResult = StatisticFunction.calculateMean(calculateEntropyResult)

    sourceRdd.foreach(println(_))
    lRdd.foreach(println(_))
    sensitiveInfo.foreach(println(_))
    fractionRdd.foreach(println(_))
    entropyRdd.foreach(println(_))
    calculateEntropyResult.foreach(println(_))
    fractionStatisticResult.foreach(println(_))
    sensitiveInfoResult.foreach(println(_))
    println(entropyMaxResult)
    println(entropyMinResult)
    println(entropyMeanResult)
    sc.stop()

  }
}
