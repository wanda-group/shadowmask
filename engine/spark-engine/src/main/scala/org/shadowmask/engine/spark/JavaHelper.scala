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
package org.shadowmask.engine.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkContext}
import org.shadowmask.engine.spark.partitioner.RolbinPartitioner



object JavaHelper {


  // rdd repartition
  def rddRepartition[T](rdd: RDD[T], partitions: Int): RDD[T] = rdd.repartition(partitions)

  def rddRepartition[T](sc: SparkContext, rdd: RDD[T], partitions: Int, preferLocation: String)(p: Partitioner = new RolbinPartitioner(partitions)): RDD[String] = {
    var partitionList = new Array[List[T]](partitions);
    (0 until partitions).foreach(i => partitionList(i) = List[T]())
    rdd.collect().foreach(item => {
      val index = p.getPartition(item)
      partitionList = partitionList.updated(index, item :: partitionList(index))
    })
    val newRdd  = sc.makeRDD(partitionList.map((_, Seq(preferLocation)))).flatMap(s => s.map(_.toString))
    newRdd
  }

}
