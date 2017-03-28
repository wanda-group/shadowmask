package org.shadowmask.engine.spark.hierarchy

import org.junit.Test
import org.junit.Assert._
import org.shadowmask.engine.spark.hierarchy.impl.IdHierarchy

/**
  * Created by qianhuizi on 2017/3/22.
  */
class IdHierarchyTest {

  @Test
  def testIdHierarchy(): Unit = {
    val data = Array(Array("340001199908180022", "340001199908180022", "340001199908180022", "340001199908180022", "340001199908180022"))
    val hierarchy: IdHierarchy = new IdHierarchy(true,true,'*')
    val rule0: (String) => String = hierarchy.getIdRule(0)
    assertEquals("340001199908180022", rule0("340001199908180022"))
    val rule1: (String) => String = hierarchy.getIdRule(1)
    assertEquals("34000119990818002x", rule1("340001199908180022"))
    val rule2 = hierarchy.getIdRule(2)
    assertEquals("34000119990818002X", rule2("340001199908180022"))
    val rule3 = hierarchy.getIdRule(3)
    assertEquals("34000119990818002a", rule3("340001199908180022"))
  }

}
