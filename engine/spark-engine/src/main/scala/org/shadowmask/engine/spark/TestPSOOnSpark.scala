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

import java.io.FileInputStream
import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.shadowmask.core.data.DataType
import org.shadowmask.core.domain.TaxTreeFactory
import org.shadowmask.core.domain.tree.{CategoryTaxTree, IntegerTaxTree, TaxTree, TaxTreeNode}
import org.shadowmask.engine.spark.autosearch.pso.SparkDrivedDataAnoymizePSOSearch
import org.shadowmask.engine.spark.autosearch.pso.SparkDrivedDataAnoymizePSOSearch.RddReplicatedSparkDrivedFitnessCalculator
import org.shadowmask.engine.spark.autosearch.pso.cluster.CombineClusterMkVelocityCalculator


object TestPSOOnSpark {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("shadowmask").setMaster("spark://testbig1.wanda.cn:7077")
    val conf = new SparkConf().setAppName("shadowmask").setMaster(args(0))
    conf.set("spark.defalut.parallelism", args(3))
    val sc = new SparkContext(conf)
    val sourceRdd = sc.textFile(args(1) + "/adult.data", args(4).toInt).filter(_.trim.length > 0)
      .map(s => {
        val sp = s.split(",")
        Array[String](sp(0), sp(1), sp(3), sp(5), sp(8), sp(9), sp(13), sp(14), sp(6)).map(_.trim).mkString(",")
      })
//      .repartition(args(5).toInt)
    //      .cache()
    val contryTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    contryTree.constructFromYamlInputStream(new FileInputStream(args(2) + "/contry.yaml"))
    //    contryTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream(args(1) + "/contry.yaml"))
    val eduTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    eduTree.constructFromYamlInputStream(new FileInputStream(args(2) + "/education.yaml"))
    //    eduTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream(args(1) + "/education.yaml"))
    val maritalTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    maritalTree.constructFromYamlInputStream(new FileInputStream(args(2) + "/marital.yaml"))
    //    maritalTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream(args(1) + "/marital.yaml"))
    val raceTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    raceTree.constructFromYamlInputStream(new FileInputStream(args(2) + "/race.yaml"))
    //    raceTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream(args(1) + "/race.yaml"))
    val generTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    generTree.constructFromYamlInputStream(new FileInputStream(args(2) + "/gender.yaml"))
    //    generTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream(args(1) + "/gender.yaml"))
    val workClass = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    workClass.constructFromYamlInputStream(new FileInputStream(args(2) + "/workclass.yaml"))
    //    workClass.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream(args(1) + "/workclass.yaml"))


    val count = sc.broadcast(sourceRdd.count())
    val ageTree = TaxTreeFactory.getTree[IntegerTaxTree](DataType.INTEGER)
    val it = sourceRdd.map(_.split(",")(0).toInt).sortBy(_.toInt, true).toLocalIterator

    ageTree.buildFromSortedValues(new java.util.Iterator[Integer] {
      override def next(): Integer = it.next()

      override def hasNext: Boolean = it.hasNext
    }, count.value.toInt, 10, Array(3, 1))


    val trees = Array[TaxTree[_ <: TaxTreeNode]](contryTree, eduTree, maritalTree, raceTree, generTree, workClass, ageTree)


    val separator = sc.broadcast(",")

    val index: java.util.Map[Integer, Integer] = new util.HashMap[Integer, Integer]()

    /*
      0 - 0 - age
      1 - 1 - work
      2 - 3 - edu
      3 - 5 - married
      4 - 8 - race
      5 - 9 - gender
      6 - 13- contry
     */

    index.put(0, 6)
    index.put(1, 5)
    index.put(2, 1)
    index.put(3, 2)
    index.put(4, 3)
    index.put(5, 4)
    index.put(6, 0)

    val indexBrodcast = sc.broadcast(index)

    val searcher = new SparkDrivedDataAnoymizePSOSearch(args(6).toInt)
    searcher.setCalculator(new RddReplicatedSparkDrivedFitnessCalculator(args(6).toInt, args(7).toInt, args(8).toInt))
    searcher.setSc(sc)
    searcher.setPrivateTable(sourceRdd)
    searcher.setTrees(trees)

    searcher.setCalculator(new CombineClusterMkVelocityCalculator)
    searcher.setTreeHeights(trees.map(_.getHeight))
    searcher.setSeparator(separator)
    searcher.setDataGeneralizerIndex(indexBrodcast)
    searcher.setDataSize(count.value)
    searcher.setOutlierRate(0.001)
    searcher.setTargetK(10)
    searcher.setQusiIndexes((0 to 6).toArray)
    searcher.setDimension(7)
    searcher.setParticleSize(100)
    searcher.setMaxSteps(2000)
    searcher.init()
    searcher.optimize()

    print("finished")

    //    searcher.setS


  }
}
