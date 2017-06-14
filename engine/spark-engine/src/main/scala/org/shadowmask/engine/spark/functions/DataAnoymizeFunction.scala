/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.engine.spark.functions

import java.io.{File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.shadowmask.core.mask.rules.generalizer.actor.{GeneralizerActor, TaxTreeClusterGeneralizerActor}
import org.shadowmask.engine.spark._

import scala.collection.JavaConverters._

object DataAnoymizeFunction {


  def anoymize(sc: SparkContext, table: RDD[String], generalizers: autosearch.pso.MkParticle, fieldSeapartor: Broadcast[String], generalizerIndex: Broadcast[java.util.Map[Integer, Integer]]): RDD[String] = {
    val actors = generalizers
      .currentPosition()
      .getGeneralizerActors
    anoymizeWithActors(sc, table, actors, fieldSeapartor, generalizerIndex)
  }

  def anoymizeWithActors(sc: SparkContext, table: RDD[String], generalizers: Array[GeneralizerActor[_, _]], fieldSeapartor: Broadcast[String], generalizerIndex: Broadcast[java.util.Map[Integer, Integer]]): RDD[String] = {
    val actors = generalizers
    table.map(
      record => {
        val fieldValues = record.split(fieldSeapartor.value)
        generalizerIndex.value.asScala.foreach(
          index => {

            val actor = actors(index._2)
              .asInstanceOf[TaxTreeClusterGeneralizerActor]

            fieldValues(index._1) = actor
              .generalize(fieldValues(index._1))
          }
        )
        fieldValues.mkString(fieldSeapartor.value)
      }
    )
  }


  def utility(maskTable: RDD[String], targetK: Int, count: Long, outlierRate: Double, separator: String,
              qusiIdIdx: Array[Int], IdIdx: Array[Int] = Array(), sensitiveIdx: Array[Int] = Array()): (Double, Long, Array[Int]) = {

    val eqClassSize = maskTable.map(
      record => {
        val sp = record.split(separator)
        (qusiIdIdx.map(sp(_)).mkString(separator), 1)
      }
    ).reduceByKey(_ + _).cache()


    val ks = eqClassSize.collect().map(_._2)
    //    eqClassSize.sortBy(_._2, false).collect().foreach(println _)
    print(ks)
    val outlierSize: Long = eqClassSize.map(kv => if (kv._2 < targetK) kv._2.toLong else 0.toLong).reduce(_ + _)
    val actOutlierRate = outlierSize.toDouble / count.toDouble

    val utilityValue = eqClassSize.map(cnt => if (cnt._2 >= targetK.toLong) cnt._2 * cnt._2 else (targetK - cnt._2) * count).reduce(_ + _)

    (actOutlierRate, utilityValue, ks)
  }


  def anoymizeWithRecordDepth(sc: SparkContext, table: RDD[String], generalizers: autosearch.pso.MkParticle, fieldSeapartor: Broadcast[String], generalizerIndex: Broadcast[java.util.Map[Integer, Integer]]): RDD[(String, (Integer, Integer))] = {
    val actors = generalizers
      .currentPosition()
      .getGeneralizerActors

    table.map(
      record => {
        val fieldValues = record.split(fieldSeapartor.value)
        var depthSum = 0
        var maxDepthSum = 0
        generalizerIndex.value.asScala.foreach(
          index => {
            val actor = actors(index._2)
              .asInstanceOf[TaxTreeClusterGeneralizerActor]
            fieldValues(index._1) = actor
              .generalize(fieldValues(index._1))
            depthSum += actor.getCurrentMaskLevel
            maxDepthSum += actor.getMaxLevel
          }
        )
        (fieldValues.mkString(fieldSeapartor.value), (depthSum, maxDepthSum))
      }
    )
  }


  def utilityWithRecordDepth(maskTable: RDD[(String, (Integer, Integer))], targetK: Int, count: Long, outlierRate: Double, separator: String,
                             qusiIdIdx: Array[Int], IdIdx: Array[Int] = Array(), sensitiveIdx: Array[Int] = Array()): (Double, Long, Array[Int]) = {

    val eqClassDepthSumKValue = maskTable.map(
      record => {
        val sp = record._1.split(separator)
        (qusiIdIdx.map(sp(_)).mkString(separator), (record._2._1, record._2._2, 1))
      }
    ).reduceByKey((depthNum1, depthNum2) => (depthNum1._1 + depthNum2._1, depthNum1._2 + depthNum2._2, depthNum1._3 + depthNum2._3)).cache()

    //(depthsum,maxDepthSum,Kvalue)


    val outlierSize: Long = eqClassDepthSumKValue.map(kv => if (kv._2._3 < targetK) kv._2._3.toLong else 0.toLong).reduce(_ + _)
    val actOutlierRate = outlierSize.toDouble / count.toDouble

    val fitnessValue = eqClassDepthSumKValue.map(_._2).map(
      sumAndK => {
        if (sumAndK._3 >= targetK) sumAndK._1.toLong *
          (if (sumAndK._3 - targetK > targetK) targetK else sumAndK._3 - targetK) else sumAndK._2.toLong * sumAndK._3 * (targetK - sumAndK._3)
      }
    ).reduce(_ + _)

    val ks = eqClassDepthSumKValue.collect().map(_._2._3)

    (actOutlierRate, fitnessValue, ks)

  }


  def utilityWithEntropyTableWithDepth(maskTable: RDD[(String, (Integer, Integer))], targetK: Int, count: Long, separator: String, qusiIdIdx: Array[Int]): (java.lang.Double, Integer, Integer, Integer, Array[String]) = {
    val eqClasses = maskTable
      .map(
        // select qusi is
        r => {
          val sp = r._1.split(separator)
          (qusiIdIdx.map(sp(_)).mkString(separator), (1, r._2._1, r._2._2))
        }
      )
      .reduceByKey(
        (c1, c2) => {
          (c1._1 + c2._1, c1._2 + c2._2, c1._3 + c2._3)
        }
      )
//    val ks = eqClasses.map(ec => ec._2 + "----" + ec._1).collect()

    val ek = eqClasses
      .map(
        r => {

          val badK = if ((r._2)._1 >= targetK) 0 else (r._2)._1
          val p = (r._2)._1.toDouble / count.toDouble
          (p * Math.log(1 / p) / Math.log(2), badK, (r._2)._2, (r._2)._3)
        }
      )
      .reduce(
        (s1, s2) => {
          (s1._1 + s2._1, s1._2 + s2._2, s1._3 + s2._3, s1._4 + s2._4)
        }
      )

    (ek._1, ek._2, ek._3, ek._4, null)
  }


  def utilityWithEntropy(maskTable: RDD[String], targetK: Int, count: Long, separator: String, qusiIdIdx: Array[Int]): (java.lang.Double, Integer, Array[String]) = {
    val eqClasses = maskTable
      .map(
        // select qusi is
        r => {
          val sp = r.split(separator)
          (qusiIdIdx.map(sp(_)).mkString(separator), 1)
        }
      )
      .reduceByKey(_ + _).cache()

    val ks = eqClasses.map(ec => ec._2 + "----" + ec._1).collect()

    val ek = eqClasses
      .map(
        r => {

          val badK = if (r._2 >= targetK) 0 else r._2

          val p = r._2.toDouble / count.toDouble
          (p * Math.log(1 / p) / Math.log(2), badK)
        }
      )
      .reduce((ek1, ek2) => {
        (ek1._1 + ek2._1, ek1._2 + ek2._2)
      })

    (ek._1, ek._2, ks)
  }


  def persistMaskResult(filePath: String, result: RDD[String]): Unit = {
    val file = new File(filePath)
    val fileWriter = new FileWriter(file)
    result.collect().foreach(s => fileWriter.append(s + "\n"))
    fileWriter.close()
  }


}
