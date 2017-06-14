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
package org.shadowmask.engine.spark.autosearch

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.shadowmask.core.data.DataType
import org.shadowmask.core.domain.TaxTreeFactory
import org.shadowmask.core.domain.tree.{CategoryTaxTree, IntegerTaxTree, TaxTree, TaxTreeNode}
import org.shadowmask.engine.spark.autosearch.pso.SparkDrivedDataAnoymizePSOSearch


object TestPSOOnSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("name").setMaster("local")
    val sc = new SparkContext(conf)
    val sourceRdd = sc.textFile(this.getClass.getClassLoader.getResource("adult.data").getPath).filter(_.trim.length > 0)
        .map(s=>{
          val sp = s.split(",")
          Array[String](sp(0), sp(1), sp(3), sp(5), sp(8), sp(9), sp(13),sp(14), sp(6)).map(_.trim).mkString(",")
        })
      .cache()
    val contryTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    contryTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/contry.yaml"))
    val eduTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    eduTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/education.yaml"))
    val maritalTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    maritalTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/marital.yaml"))
    val raceTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    raceTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/race.yaml"))
    val generTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    generTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/gender.yaml"))
    val workClass = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    workClass.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/workclass.yaml"))



    val count = sc.broadcast(sourceRdd.count())
    val ageTree = TaxTreeFactory.getTree[IntegerTaxTree](DataType.INTEGER)
    val it = sourceRdd.map(_.split(",")(0).toInt).sortBy(_.toInt,true).toLocalIterator

    ageTree.buildFromSortedValues(new java.util.Iterator[Integer] {
      override def next(): Integer = it.next()

      override def hasNext: Boolean = it.hasNext
    }, count.value.toInt, 10, Array(3, 1))


    val trees = Array[TaxTree[_ <: TaxTreeNode]](contryTree, eduTree, maritalTree, raceTree, generTree, workClass, ageTree)


    val separator = sc.broadcast(",")

    val index:java.util.Map[Integer,Integer] = new util.HashMap[Integer,Integer]()

    /*
      0 - 0 - age
      1 - 1 - work
      2 - 3 - edu
      3 - 5 - married
      4 - 8 - race
      5 - 9 - gender
      6 - 13- contry
     */

    index.put(0,6)
    index.put(1,5)
    index.put(2,1)
    index.put(3,2)
    index.put(4,3)
    index.put(5,4)
    index.put(6,0)

    val indexBrodcast = sc.broadcast(index);

    val searcher = new SparkDrivedDataAnoymizePSOSearch(2)
    searcher.setSc(sc)
    searcher.setPrivateTable(sourceRdd)
    searcher.setTrees(trees)
    searcher.setTreeHeights(trees.map(_.getHeight))
    searcher.setSeparator(separator)
    searcher.setDataGeneralizerIndex(indexBrodcast)

    searcher.init()
    searcher.optimize()
//    searcher.setS


  }
}
