package org.shadowmask.engine.spark.hierarchy

import org.junit.Test
import org.junit.Assert._
import org.shadowmask.engine.spark.hierarchy.impl.DateHierarchy
/**
  * Created by qianhuizi on 2017/3/22.
  */
class DateHierarchyTest {

  @Test
  def testDateHierarchy(): Unit = {
    val data = Array(Array("2017-03-22", "2017-03-22", "2017-03-22", "2017-03-22", "2017-03-22"))
    val hierarchy: DateHierarchy = new DateHierarchy(true,true,'*')
    val rule0: (String) => String = hierarchy.getDateRule(0)
    assertEquals("2017-03-22", rule0("2017-03-22"))
    val rule1: (String) => String = hierarchy.getDateRule(1)
    assertEquals("2017-03-01", rule1("2017-03-22"))
    val rule2 = hierarchy.getDateRule(2)
    assertEquals("2017-01-01", rule2("2017-03-22"))
    val rule3 = hierarchy.getDateRule(3)
    assertEquals("1901-01-01", rule3("2017-03-22"))
  }

}
