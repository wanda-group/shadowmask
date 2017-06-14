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
package org.shadowmask.engine.spark.taxtree

import org.junit.{Assert, Test}
import org.shadowmask.core.data.DataType
import org.shadowmask.core.domain.TaxTreeFactory
import org.shadowmask.core.domain.tree.{LeafLocator, CategoryTaxTree}
import org.shadowmask.core.mask.rules.generalizer.actor.{TaxTreeClusterGeneralizerActor, TaxTreeGeneralizerActor}


class TestTaxTreeGeneralizer {
  @Test
  def testContry(): Unit = {
    val contryTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    contryTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/contry.yaml"))
    val node = contryTree.locate("Jamaica")
    Assert.assertNotNull(node)
    Assert.assertNotNull(contryTree)
    Assert.assertEquals(node.getParent.getName, "South America")

    val generalizer = new TaxTreeGeneralizerActor[String, String]
    generalizer.withDTreeAsTax(contryTree).withMaxLevel(contryTree.getHeight - 1)
    Assert.assertNotNull(generalizer.getdTree())

    generalizer.withLevel(0)
    var res = generalizer.generalize("Jamaica")
    Assert.assertEquals(res, "Jamaica")

    generalizer.withLevel(1)
    res = generalizer.generalize("Jamaica")
    Assert.assertEquals(res, "South America")

    generalizer.withLevel(2)
    res = generalizer.generalize("Jamaica")
    Assert.assertEquals(res, "other")

    generalizer.withLevel(3)
    res = generalizer.generalize("Jamaica")
    Assert.assertEquals(res, "*")

    generalizer.withLevel(30)
    res = generalizer.generalize("Jamaica")
    Assert.assertEquals(res, "*")

//    val clusterGeneralizer = new TaxTreeClusterGeneralizerActor
//      .withTree(contryTree.asInstanceOf[LeafLocator]).withMasterGeneralizer(generalizer)
//    clusterGeneralizer.getMasterGeneralizer.asInstanceOf[TaxTreeGeneralizerActor[String, String]].withLevel(0)
//
//    clusterGeneralizer.getSlaveMap
//      .put(contryTree.locate("China")
//        , generalizer.newInstance()
//          .withDTreeAsTax(contryTree)
//          .withMaxLevel(contryTree.getHeight - 1).withLevel(2)
//      )
//    res = clusterGeneralizer.generalize("Jamaica")
//    Assert.assertEquals(res, "Jamaica")
//    res = clusterGeneralizer.generalize("China")
//    Assert.assertEquals(res, "other")
  }

  @Test
  def testEdu(): Unit = {
    val eduTree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    eduTree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/education.yaml"))
    val node = eduTree.locate("Preschool")
    Assert.assertNotNull(node)
    Assert.assertNotNull(eduTree)
    Assert.assertEquals(node.getParent.getName, "0~8th")
  }

  @Test
  def testMarital(): Unit = {
    val tree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    tree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/marital.yaml"))
    val node = tree.locate("Separated")
    Assert.assertNotNull(node)
    Assert.assertNotNull(tree)
    Assert.assertEquals(node.getParent.getName, "port single")
  }

  @Test
  def testRace(): Unit = {
    val tree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    tree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/race.yaml"))
    val node = tree.locate("Amer-Indian-Eskimo")
    Assert.assertNotNull(node)
    Assert.assertNotNull(tree)
    Assert.assertEquals(node.getParent.getName, "color")
  }

  @Test
  def testGender(): Unit = {
    val tree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    tree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/gender.yaml"))
    val node = tree.locate("Female")
    Assert.assertNotNull(node)
    Assert.assertNotNull(tree)
    Assert.assertEquals(node.getParent.getName, "*")
  }

  @Test
  def testWorkClass(): Unit = {
    val tree = TaxTreeFactory.getTree[CategoryTaxTree](DataType.STRING)
    tree.constructFromYamlInputStream(this.getClass.getClassLoader.getResourceAsStream("tax/workclass.yaml"))
    val node = tree.locate("Federal-gov")
    Assert.assertNotNull(node)
    Assert.assertNotNull(tree)
    Assert.assertEquals(node.getParent.getName, "Government")
  }

}
