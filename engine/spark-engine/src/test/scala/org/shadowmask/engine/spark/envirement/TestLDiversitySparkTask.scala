package org.shadowmask.engine.spark.envirement

import org.apache.spark.{SparkConf, SparkContext}
import org.shadowmask.core.AnonymityFieldType
import org.shadowmask.engine.spark.functions.DataAnoymizeFunction.lDiversityCompute

import scala.collection.mutable.Map


object TestLDiversitySparkTask {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local")
    val sc = new SparkContext(conf)
    val sourceRdd = sc.textFile(this.getClass.getClassLoader.getResource("adult.data").getPath)
    val dataSize = sourceRdd.count()
    val fieldSeapartor = sc.broadcast(",")

    var gMap: Map[Int, AnonymityFieldType] = Map()
    gMap += (0 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (1 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (3 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (5 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (8 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (9 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (13 -> AnonymityFieldType.QUSI_IDENTIFIER)
    gMap += (14 -> AnonymityFieldType.SENSITIVE)
    gMap += (6 -> AnonymityFieldType.SENSITIVE)

    val lRdd = lDiversityCompute(sc, sourceRdd, gMap, fieldSeapartor)
    lRdd.foreach(println(_))

  }
}
