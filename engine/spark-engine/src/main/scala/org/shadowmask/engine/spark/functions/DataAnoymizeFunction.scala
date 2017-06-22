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

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.shadowmask.core.mask.rules.generalizer.actor.TaxTreeClusterGeneralizerActor
import org.shadowmask.engine.spark._
import org.shadowmask.core.AnonymityFieldType

import scala.collection.JavaConverters._
import scala.collection.mutable._

object DataAnoymizeFunction {


  def anoymize(sc: SparkContext, table: RDD[String], generalizers: autosearch.pso.MkParticle, fieldSeapartor: Broadcast[String], generalizerIndex: Broadcast[java.util.Map[Integer, Integer]]): RDD[String] = {


    val actors = generalizers
      .currentPosition()
      .getGeneralizerActors

    table.map(
      record => {
        val fieldValues = record.split(fieldSeapartor.value)
        generalizerIndex.value.asScala.foreach(
          index => {
            fieldValues(index._1) = actors(index._2)
              .asInstanceOf[TaxTreeClusterGeneralizerActor]
              .generalize(fieldValues(index._1))
          }
        )
        fieldValues.mkString(fieldSeapartor.value)
      }
    )

  }

  def lDiversityCompute(sc: SparkContext, sourceRdd: RDD[String], gMap: Map[Int, AnonymityFieldType], fieldSeapartor: Broadcast[String]): RDD[String] = {
    var lParam = 0
    val gMapArray = gMap.toArray
    var quasiIdentifier: List[Int] = List()
    var sensitiveInfo: List[Int] = List()
    sc.parallelize(gMapArray).map(tuple => {
      val fieldKey = tuple._2
      val fieldValue = tuple._1
      (fieldKey, fieldValue)
    })
      .groupByKey()
      .collect()
      .map(tu => {
        if (tu._1.toString.equals("QUSI_IDENTIFIER")) {
          quasiIdentifier ++= tu._2.toList.sortWith(_ < _)
          quasiIdentifier
        } else if (tu._1.toString.equals("SENSITIVE")) {
          sensitiveInfo ++= tu._2.toList.sortWith(_ < _)
          sensitiveInfo
        }
      })
    val selectRdd =
      sourceRdd.filter(_.trim.length > 0).map(s => {
        val sp = s.split(fieldSeapartor.value).map(_.trim)
        val quasiIdentifierArray: Array[Int] = quasiIdentifier.toArray
        val sensitiveInfoArray: Array[Int] = sensitiveInfo.toArray
        var quasiIdentifierString: String = ""
        var sensitiveInfoString: String = ""

        for (i <- quasiIdentifierArray) {
          quasiIdentifierString += sp(i) + fieldSeapartor.value
        }
        for (i <- sensitiveInfoArray) {
          sensitiveInfoString += sp(i) + fieldSeapartor.value
        }
        (quasiIdentifierString, sensitiveInfoString)
      })
        .cache()

    val count = sc.broadcast(selectRdd.count())
    val targetl = sc.broadcast(lParam)
    val tableSize = sc.broadcast(count)

    val selectRddCompute: RDD[String] = selectRdd.map(s => ((s._1), s._2))
      .distinct()
      .groupByKey()
      .map(tu => {
        val key = tu._1
        val values = tu._2
        lParam = values.size
        ((key, values), lParam).toString()
      })
    selectRddCompute
  }
}
