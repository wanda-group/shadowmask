package org.shadowmask.engine.spark.functions

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.math.log10


object statisticFunction {
  def paramsStatistic(sc: SparkContext, sourceRdd: RDD[(String, mutable.Iterable[String])]): RDD[(Map[String, Double], List[(String, Double)])] = {
    val fractionRdd: RDD[Map[(String, String), Double]] = sourceRdd.map({
      tuple =>
        val keys = tuple._1
        val values = tuple._2
        var resultKey: String = ""
        var resultValue: Double = 0
        var map = Map.empty[String, Double]
        var fractionMap = Map.empty[(String, String), Double]
        var sumTotal: Double = 0
        var total: Double = 0

        for (value <- values) {
          total = total + 1
          if (map.contains(value)) {
            map += (value -> (map(value) + 1))
          }
          else {
            map += (value -> 1)
          }
        }
        map.keys.foreach { i =>
          sumTotal = sumTotal + map(i)
        }
        map.keys.foreach { i =>
          resultKey = i
          resultValue = map(i) / sumTotal
          fractionMap += ((keys, resultKey) -> resultValue)
        }
        fractionMap
    }
    )
    //    fractionRdd.foreach(println(_))

    def fractionStatistics(rddFraction: RDD[Map[(String, String), Double]]) = rddFraction.map(
      {
        tuple =>
          val fractionMax = tuple.values.max
          val fractionMin = tuple.values.min
          val fractionMean = (tuple.values.reduce(_ + _)) / (tuple.values.size)
          (fractionMax, fractionMin, fractionMean)
      }
    )

    val resultRdd: RDD[(Map[String, Double], List[(String, Double)])] = fractionRdd.map(
      {
        var entropyMap: Map[String, Double] = Map()
        tuple =>
          var resultList: List[(String, Double)] = List()
          for (elem <- tuple.keys) {

            tuple.values.map {
              j =>
                val value = j
                val entropyResult = -value * log10(value) / log10(2)
                entropyMap += (elem._1 -> entropyResult)
            }
          }
          //          entropyMap.foreach(println(_))
          val entropyMax = entropyMap.toList.reduce((r1, r2) => {

            if (r1._2 < r2._2) r2 else r1
          })
          val entropyMin = entropyMap.toList.reduce((r1, r2) => {
            if (r1._2 > r2._2) r2 else r1
          })
          val entropyMean = entropyMap.toList.reduce((r1, r2) => {
            val result = (r1._2 + r2._2) / entropyMap.toList.size
            ("MeanValue", result)
          })
          resultList = List(entropyMax, entropyMin, entropyMean)
          (entropyMap, resultList)
      }
    )

    fractionCalculate(fractionRdd)

    def fractionCalculate(rddCalculate: RDD[Map[(String, String), Double]]): (RDD[(mutable.Map[String, Double], List[(String, Double)])], List[RDD[(Double, Double, Double)]]) = {
      val stat = fractionStatistics(rddCalculate)
      val listResult = List(stat)
      (resultRdd, listResult)
    }

    resultRdd
  }
}
