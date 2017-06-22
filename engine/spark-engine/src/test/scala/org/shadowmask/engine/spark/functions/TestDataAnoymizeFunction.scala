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

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.{Assert, Test}

class TestDataAnoymizeFunction {


  @Test
  def testUtilityWithEntropy(): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local")
    val sc = new SparkContext(conf)

    //原始数据表
    val data = Array(
      "上海,*;男",
      "上海,*;男",
      "北京,*;女",
      "北京,*;女",
      "北京,*;女"
    )
    //原始数据表
    val rdd = sc.parallelize(data)

    //原始数据表
    val data1 = Array(
      "上海,浦江;男",
      "上海,浦江;男",
      "北京,朝阳;女",
      "北京,东城;女",
      "北京,东城;女"
    )
    //原始数据表
    val rdd1 = sc.parallelize(data1)
    val e = DataAnoymizeFunction.utilityWithEntropy(rdd, 1,rdd.count().toLong, ";", Array(0, 1))
    val e1 = DataAnoymizeFunction.utilityWithEntropy(rdd1,1, rdd1.count().toLong, ";", Array(0, 1))
    Assert.assertTrue(e._1 <= e1._1)
  }


}
