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

package org.shadowmask.engine.spark.envirement

import org.apache.spark.{SparkConf, SparkContext}
import org.shadowmask.core.mask.rules.generalizer.Generalizer


object TestMask {

  def main(args: Array[String]) = {
    val conf = new SparkConf().setAppName("name").setMaster("local")
    val sc = new SparkContext(conf)

    //原始数据表
    val data = Array(
      "上海,闵行|男",
      "上海,浦东新区|男",
      "上海,普陀区|男",
      "北京,东城|女",
      "北京,朝阳|女",
      "深圳,罗湖区|女"
    )
    //原始数据表
    val rdd = sc.parallelize(data)
    // 定义两个generalizer
    val generalizer1 = new AddrGeneralizer
    val generalizer2 = new GenderGenereralizer
    //表的规模
    val count = rdd.count();
    //目标K值
    val targetk = 2

    var gMap:Map[Int,Generalizer[String,String]]  = Map()
    gMap += (1->generalizer1)
    gMap += (3->generalizer2)


    //脱敏
    val maskedTable = rdd.map(str => {


      val columns = str.split("\\|");
      (
        (generalizer1.generalize(columns(0), 1), generalizer2.generalize(columns(1), 1)),
        1
      ) // ((上海,*),1),
    })

    maskedTable.cache()


    //查看脱敏的结果
    maskedTable.collect().foreach(value => println(s"${value._1._1},${value._1._2}"))
    //统计K值
    val minClass = maskedTable
      .reduceByKey(_ + _) // 代替group by
      .reduce((r1, r2) => { //区最小的等价类
        if (r1._2 > r2._2) r2 else r1
      })
    println(minClass._1)
    println(minClass._2)

    //广播变量
    val kValue = sc.broadcast(targetk)
    val tableSize = sc.broadcast(count)

    //统计惩罚因子
    val fac = maskedTable
      .reduceByKey(_ + _)
      .map(value => if (value._2 >= kValue.value) value._2 * value._2 else value._2 * tableSize.value)
      .reduce(_ + _)

    println(fac)


  }


  class AddrGeneralizer extends Generalizer[String, String] {

    override def generalize(input: String, hierarchyLevel: Int): String = input.split(",")(0)

    override def getRootLevel: Int = 0
  }

  class GenderGenereralizer extends Generalizer[String, String] {

    override def generalize(input: String, hierarchyLevel: Int): String = "*"

    override def getRootLevel: Int = 0
  }

}
