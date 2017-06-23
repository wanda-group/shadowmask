package org.shadowmask.engine.spark.hierarchy.impl

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.shadowmask.engine.spark.hierarchy.Hierarchy
import org.shadowmask.engine.spark.hierarchy.mask.DateRule

class DateHierarchy(alignLeft: Boolean,
                     maskLeft: Boolean,
                     maskChar: Char = '*') extends Hierarchy[String, String]{

  override def rootHierarchyLevel: Int = -1

  override def getUDF(hierarchy: Int): UserDefinedFunction = udf(getDateRule(hierarchy))

  def getDateRule(hierarchy: Int): (String) => String = {
    new DateRule(hierarchy, maskChar).mask
  }
}




