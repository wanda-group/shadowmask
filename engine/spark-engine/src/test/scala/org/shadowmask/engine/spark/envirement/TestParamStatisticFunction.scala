package org.shadowmask.engine.spark.envirement

import org.shadowmask.core.AnonymityFieldType
import org.shadowmask.engine.spark.functions.DataAnoymizeFunction.lDiversityCompute
import org.shadowmask.engine.spark.functions.statisticFunction
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
    val result = statisticFunction.paramsStatistic(sc, lRdd)

    result.foreach(println(_))
    sc.stop()

  }
}
