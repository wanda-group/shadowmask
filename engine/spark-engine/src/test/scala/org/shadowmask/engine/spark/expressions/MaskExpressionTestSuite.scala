package org.shadowmask.engine.spark.expressions

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.expressions.MaskExpressions._
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.junit.Assert.assertEquals
import org.junit._

class MaskExpressionTestSuite {

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
  def testIPMaskExpression: Unit = {
    val maskedIps = paymentData.select(ipMask(new Column("pos"), 2))
      .collect()
      .map(_.getString(0))
      .mkString(",")
    assertEquals("10.199.*.*,10.199.*.*", maskedIps)
  }
}
