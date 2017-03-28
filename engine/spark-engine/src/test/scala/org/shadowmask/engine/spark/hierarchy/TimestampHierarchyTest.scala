package org.shadowmask.engine.spark.hierarchy

import org.junit.Test
import org.junit.Assert._
import org.shadowmask.engine.spark.hierarchy.impl.{IdHierarchy, TimestampHierarchy}

/**
  * Created by qianhuizi on 2017/3/22.
  */
class TimestampHierarchyTest {

  @Test
  def testTimestampHierarchy(): Unit = {
    val data = Array(Array("2016-09-18T10:30:32.000", "2016-09-18T10:30:32.000", "2016-09-18T10:30:32.000", "2016-09-18T10:30:32.000", "2016-09-18T10:30:32.000"))
    val hierarchy: TimestampHierarchy = new TimestampHierarchy(true,true,'*')
    val rule0: (String) => String = hierarchy.getTimestampRule(0)
    assertEquals("2016-09-18T10:30:32.000", rule0("2016-09-18T10:30:32.000"))
    val rule1: (String) => String = hierarchy.getTimestampRule(1)
    assertEquals("2016-09-18T10:30:32", rule1("2016-09-18T10:30:32.000"))
    val rule2 = hierarchy.getTimestampRule(2)
    assertEquals("2016-09-18T10:30:00", rule2("2016-09-18T10:30:32.000"))
    val rule3 = hierarchy.getTimestampRule(3)
    assertEquals("2016-09-18T10:00:00", rule3("2016-09-18T10:30:32.000"))
    val rule4 = hierarchy.getTimestampRule(4)
    assertEquals("2016-09-18T00:00:00", rule4("2016-09-18T10:30:32.000"))
    val rule5 = hierarchy.getTimestampRule(5)
    assertEquals("2016-09-01T00:00:00", rule5("2016-09-18T10:30:32.000"))
    val rule6 = hierarchy.getTimestampRule(6)
    assertEquals("2016-01-01T00:00:00", rule6("2016-09-18T10:30:32.000"))
    val rule7 = hierarchy.getTimestampRule(7)
    assertEquals("1901-01-01T00:00:00", rule7("2016-09-18T10:30:32.000"))
  }

}
