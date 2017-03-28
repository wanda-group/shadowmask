package org.shadowmask.engine.spark.hierarchy


import org.junit.Test
import org.junit.Assert._
import org.shadowmask.engine.spark.hierarchy.impl.EmailHierarchy
/**
  * Created by qianhuizi on 2017/3/22.
  */
class EmailHierarchyTest {

  @Test
  def testEmailHierarchy(): Unit = {
    val data = Array(Array("zhangsan1@gmail.com", "zhangsan1@gmail.com", "zhangsan1@gmail.com", "zhangsan1@gmail.com", "zhangsan1@gmail.com"))
    val hierarchy: EmailHierarchy = new EmailHierarchy(true,true,'*')
    val rule0: (String) => String = hierarchy.getEmailRule(0)
    assertEquals("zhangsan1@gmail.com", rule0("zhangsan1@gmail.com"))
    val rule1: (String) => String = hierarchy.getEmailRule(1)
    assertEquals("*@gmail.com", rule1("zhangsan1@gmail.com"))
    val rule2 = hierarchy.getEmailRule(2)
    assertEquals("*@*.com", rule2("zhangsan1@gmail.com"))
    val rule3 = hierarchy.getEmailRule(3)
    assertEquals("*@*.*", rule3("zhangsan1@gmail.com"))
  }

}
