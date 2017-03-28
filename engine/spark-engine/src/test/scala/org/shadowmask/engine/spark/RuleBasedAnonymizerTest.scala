/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.shadowmask.engine.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.junit.{After, Before, Test}
import org.shadowmask.core.AnonymityFieldType
import org.shadowmask.engine.spark.hierarchy.aggregator.AggregatorType
import org.shadowmask.engine.spark.hierarchy.impl.{ArrayHierarchy, EmailHierarchy, IntervalHierarchy, MaskHierarchy, DateHierarchy, IdHierarchy, TimestampHierarchy}

class RuleBasedAnonymizerTest {

  private val paymentCsvFile = "src/test/resources/payment.csv"
  private var sqlContext: SQLContext = _

  @Before
  def initiate: Unit = {
    sqlContext = new SQLContext(new SparkContext("local[2]", "Csvsuite"))
  }

  @After
  def close: Unit = {
    sqlContext.sparkContext.stop()
  }

  def paymentData: DataFrame = {
    sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "false")
      .load(paymentCsvFile)
  }

  @Test
  def testSimple(): Unit = {
    val TimestampHierarchy = new TimestampHierarchy(true, true)
    val IdHierarchy = new IdHierarchy(true, true)
    val DateHierarchy = new DateHierarchy(true, true)
    val EmailHierarchy = new EmailHierarchy(true, true)
    val ipHierarchy = new MaskHierarchy(true, true)
    val ageHierachy = new IntervalHierarchy[Int](10, 60, 10, 2)


    val hierarchy1 = Array[String]("021-66889898", "021-********", "***-********")
    val hierarchy2 = Array[String]("021-66889899", "021-********", "***-********")
    val hierarchy3 = Array[String]("021-66882898", "021-********", "***-********")
    val hierarchy4 = Array[String]("021-66889899", "021-********", "***-********")
    val hierarchy5 = Array[String]("021-66883498", "021-********", "***-********")
    val hierarchy6 = Array[String]("021-66823899", "021-********", "***-********")
    val arrayHierarchy = Array(hierarchy1, hierarchy2, hierarchy3, hierarchy4, hierarchy5, hierarchy6)
    val phoneHierachy = new ArrayHierarchy(arrayHierarchy)


    val timestampFieldAttr = FieldAttribute.create("timestamp", AnonymityFieldType.QUSI_IDENTIFIER, TimestampHierarchy.getUDF(2))
    val idFieldAttr = FieldAttribute.create("id0", AnonymityFieldType.QUSI_IDENTIFIER, IdHierarchy.getUDF(1))
    val dateFieldAttr = FieldAttribute.create("date", AnonymityFieldType.QUSI_IDENTIFIER, DateHierarchy.getUDF(3))
    val emailFieldAttr = FieldAttribute.create("email", AnonymityFieldType.QUSI_IDENTIFIER, EmailHierarchy.getUDF(2))
    val posFieldAttr = FieldAttribute.create("pos", AnonymityFieldType.QUSI_IDENTIFIER, ipHierarchy.getUDF(2))
    val phoneFieldAttr = FieldAttribute.create("phone", AnonymityFieldType.QUSI_IDENTIFIER, phoneHierachy.getUDF(1))
    val ageFieldAttr = FieldAttribute.create("age", AnonymityFieldType.QUSI_IDENTIFIER, ageHierachy.getUDF(1))
    val mobileFieldAttr = FieldAttribute.create("mobile", AnonymityFieldType.QUSI_IDENTIFIER, AggregatorType.Mean)
    val locationFieldAttr = FieldAttribute.create("location", AnonymityFieldType.NON_SENSITIVE)
    val fieldAttrs = Array[FieldAttribute](timestampFieldAttr,idFieldAttr,dateFieldAttr, emailFieldAttr, posFieldAttr, phoneFieldAttr, ageFieldAttr, mobileFieldAttr, locationFieldAttr)

    val config = new AnonymizationConfig
    config.setFieldAttribute(fieldAttrs)

    RuleBasedAnonymizer(paymentData, config).anonymize()
  }
}
