package org.shadowmask.engine.spark.functions

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.Map
import scala.math.log10


object statisticFunction {
  def paramsStatistic(sc: SparkContext, sourceRdd: RDD[(String, mutable.Iterable[String])]): (RDD[(String, mutable.Iterable[String])], RDD[Map[(String, String), Double]], RDD[Map[String, Double]], RDD[(Iterable[(String, String)], Double, Double, Double)]) = {
    def convertIntToFraction(sourceRdd: RDD[(String, mutable.Iterable[String])]): RDD[Map[(String, String), Double]] = {
      sourceRdd.map({
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
    }

    def convertFractionToEntropy(fractionRdd: RDD[Map[(String, String), Double]]): RDD[Map[String, Double]] = {
      var entropyResult: Double = 0
      var entropyMap: Map[String, Double] = Map()
      fractionRdd.map(
        {
          tuple =>
            for (elem <- tuple.keys) {
              tuple.values.map {
                j =>
                  val value = j
                  entropyResult = -value * log10(value) / log10(2)

              }
              entropyMap += (elem._1 -> entropyResult)
            }
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
            var resultList: List[(String, Double)] = List()
            resultList = List(entropyMax, entropyMin, entropyMean)
            resultList.foreach(println(_))
            entropyMap
        }
      )

    }

    def fractionStatistics(rddFraction: RDD[Map[(String, String), Double]]) = rddFraction.map(
      {
        tuple =>
          val fractionMax = tuple.values.max
          val fractionMin = tuple.values.min
          val fractionMean = (tuple.values.reduce(_ + _)) / (tuple.values.size)
          (tuple.keys, fractionMax, fractionMin, fractionMean)
      }
    )


    def fractionCalculate(rddCalculate: RDD[Map[(String, String), Double]]): RDD[(Iterable[(String, String)], Double, Double, Double)] = {
      val stat = fractionStatistics(rddCalculate)
      stat
    }

    val fractionRdd = convertIntToFraction(sourceRdd)
    val entropyRdd = convertFractionToEntropy(fractionRdd)
    val fractionStatisticResult = fractionCalculate(fractionRdd)

    sourceRdd.foreach(println(_))
    fractionRdd.foreach(println(_))
    entropyRdd.foreach(println(_))
    fractionStatisticResult.foreach(println(_))
    (sourceRdd, fractionRdd, entropyRdd, fractionStatisticResult)
  }
}
