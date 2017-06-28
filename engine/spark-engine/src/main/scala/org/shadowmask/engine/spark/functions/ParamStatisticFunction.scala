package org.shadowmask.engine.spark.functions

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.math.log10


object ParamStatisticFunction {


  def statisticCaculate(sc: SparkContext, sourceRdd: RDD[(String, String)]): (RDD[((String, (String, Long)), Double)], List[Any]) = {
    val lRdd = sourceRdd
    val rddCountByKey = sc.makeRDD(lRdd.countByKey().toList)
    val rddJoin = lRdd.join(rddCountByKey)
    val rddCountByValue = sc.makeRDD(rddJoin.countByValue().toList)
    val fractionResult: RDD[((String, (String, Long)), Double)] = rddCountByValue.map({
      fractionTuple =>
        val fractionKey = fractionTuple._1
        val numerator: Double = fractionTuple._2
        val denominator: Double = fractionTuple._1._2._2
        val fraction = numerator / denominator
        (fractionKey, fraction)
    })
    fractionResult.foreach(println(_))
    val resultRdd: RDD[((String, (String, Long)), Double)] = fractionResult.map(
      {
        tuple =>
          val key = tuple._1
          val value: Double = tuple._2
          val entropyResult = -value * log10(value) / log10(2)
          (key, entropyResult)
      }
    )
    resultRdd.foreach(println(_))
    val rddStatistic = resultRdd.map({
      tuple3 =>
        val key3 = tuple3._2
        val value3 = tuple3._1

        (key3, value3)
    })
    val entropyMax = rddStatistic.max()
    val entropyMin = rddStatistic.min()
    val fractionResultMax = fractionResult.max()
    val fractionResultMin = fractionResult.min()
    val entropyRddKeyChange: RDD[(String, Double)] = resultRdd.map({
      tuple5 =>
        val key5 = tuple5._1.toString()
        val value5 = tuple5._2
        (key5, value5)
    })
    val fractionRddKeyChange: RDD[(String, Double)] = fractionResult.map({
      tuple5 =>
        val key5 = tuple5._1.toString()
        val value5 = tuple5._2
        (key5, value5)


    })
    caculateMean(entropyRddKeyChange)
    caculateMean(fractionRddKeyChange)

    def caculateMean(rdd: RDD[(String, Double)]): Double = {
      val total = rdd.reduceByKey(_ + _).values.sum()
      val average = total / rdd.count()
      average
    }

    //    def entropy(source: String): Double = {
    //      source
    //        .map(p => -p * log10(p) / log10(2))
    //        .sum
    //    }

    val listResult = List(entropyMax, entropyMin, caculateMean(entropyRddKeyChange), fractionResultMax, fractionResultMin, caculateMean(fractionRddKeyChange))
    (resultRdd, listResult)
  }
}
