package org.shadowmask.engine.spark.functions

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import org.apache.spark.util.StatCounter
import scala.math.log10


object StatisticFunction {
  def countSensitiveInfo(sc: SparkContext, source: RDD[(String, mutable.Iterable[String])]): RDD[(String, Map[String, Double])] = {
    source.map({
      tuple =>
        val keys = tuple._1
        val values = tuple._2
        var map = Map.empty[String, Double]
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
        (keys, map)
    }
    )
  }

  def convertIntToFraction(sourceRdd: RDD[(String, Map[String, Double])]): RDD[(String, Map[String, Double])] = {
    sourceRdd.map({
      tuple =>
        val keys = tuple._1
        var resultKey: String = ""
        var resultValue: Double = 0
        val map = tuple._2
        var fractionMap = Map.empty[String, Double]
        var total: Double = 0
        map.keys.foreach { i =>
          total = total + map(i)
        }
        map.keys.foreach { i =>
          resultKey = i
          resultValue = map(i) / total
          fractionMap += (resultKey -> resultValue)
          (keys, fractionMap)
        }
        (keys, fractionMap)
    }
    )
  }

  def fractionStatistics(rddFraction: RDD[(String, Map[String, Double])]) = rddFraction.map(
    {
      tuple =>
        var total = 0
        var sumValue: Double = 0
        val tu = for {
          value <- tuple._2
        } yield {
          total = total + 1
          sumValue = sumValue + value._2
          (value._2, value._1)
        }
        val fractionMax = tu.max
        val fractionMin = tu.min
        val fractionMean = sumValue / total
        (tuple._1, fractionMax, fractionMin, fractionMean)
    }
  )

  def convertFractionToEntropy(fractionRdd: RDD[(String, Map[String, Double])]): RDD[(String, Iterable[(String, Double)])] = {
    val entropyResultRdd = fractionRdd.map(
      {
        tuple =>
          val entropy = for {
            key <- tuple._2.keys
            value <- tuple._2.values
          } yield {
            (key, -value * log10(value) / log10(2))
          }
          (tuple._1, entropy)
      }
    )
    entropyResultRdd
  }

  def calculateEntropy(entropyRddEach: RDD[(String, Iterable[(String, Double)])]): RDD[(String, Double)] = entropyRddEach.map {
    tuple =>
      var sum: Double = 0
      for {
        each <- tuple._2
      } yield {
        sum += each._2
      }
      (tuple._1, sum)
  }

  def calculateMax(RddEntropyMax: RDD[(String, Double)]) = RddEntropyMax.reduce((r1, r2) => {
    if (r1._2 < r2._2) r2 else r1
  })

  def calculateMin(RddEntropyMin: RDD[(String, Double)]) = RddEntropyMin.reduce((r1, r2) => {
    if (r1._2 > r2._2) r2 else r1
  })

  def calculateAverage(RddEntropyMean: RDD[(String, Double)]): RDD[(String, Double)] = RddEntropyMean.map({
    tuple =>
      (tuple._1, average(Iterable(tuple._2)))
  })

  def average[T](ts: Iterable[T])(implicit num: Numeric[T]) = {
    num.toDouble(ts.sum) / ts.size
  }

  def calculateMean(data: RDD[(String, Double)]): RDD[(String, Double)] = {
    val dataStats = data.aggregateByKey(new StatCounter()
    )(_ merge _, _ merge _)
    val result = dataStats.map(f => (f._1, f._2.mean))
    result
  }


}
