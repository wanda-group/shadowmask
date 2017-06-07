package org.shadowmask.engine.spark.envirement

import org.apache.spark.{SparkConf, SparkContext}

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

object TestAudltSparkTask {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local")
    val sc = new SparkContext(conf)

    val sourceRdd = sc.textFile(this.getClass.getClassLoader.getResource("adult.data").getPath)

    val dataSize = sourceRdd.count();
    println(dataSize)

    // cache the data set
    val selectRdd =
      sourceRdd.filter(_.trim.length > 0).map(s => {
        val sp = s.split(",").map(_.trim)
        ((sp(0), sp(1), sp(3), sp(5), sp(8), sp(9), sp(13)), (sp(14), sp(6)))
      }).cache()

//    selectRdd.collect().foreach(println _)
//    selectRdd
//      .map(r => (r._1, 1)).reduceByKey(_ + _)
//      .sortBy(_._2, true)
//      .collect().foreach(println _)


    val count = sc.broadcast(selectRdd.count())

    selectRdd.map(s => (s._1._5, 1)).reduceByKey(_ + _).sortBy(_._2, false)
      .collect().foreach(println(_))


    //    val it = selectRdd.map(s => {
    //      s._1._1.toInt
    //    }).sortBy(_.toInt, true)
    //      .toLocalIterator
    //
    //
    //    val tree: IntegerTaxTree = new IntegerTaxTree
    //    tree.buildFromSortedValues(new util.Iterator[Integer] {
    //      override def next(): Integer = it.next()
    //
    //      override def hasNext: Boolean = it.hasNext
    //    }, count.value.toInt, 10, Array(3, 1))
    //
    //
    //    val generalizer: TaxTreeGeneralizerActor[Int, String] = new TaxTreeGeneralizerActor[Int, String]
    //    generalizer.setdTree(tree.asInstanceOf[LeafLocator[Int]])
    //    generalizer.setLevel(2)
    //    generalizer.withResultParser(new Function[TaxTreeNode, String] {
    //      override def apply(input: TaxTreeNode): String = input.getName
    //    }).withInputParser(new Function[Int, String] {
    //      override def apply(input: Int): String = input.toString
    //    })
    //    val res = generalizer.generalize(32)
    //    println(res)


  }

}
